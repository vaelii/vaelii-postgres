;; SPDX-License-Identifier: Apache-2.0
;; Copyright © 2026 Vaelii LLC and the Vaelii contributors.
(ns vaelii.postgres.record-store
  "A Postgres target for the engine's **record store** protocol
  (`vaelii.impl.protocols/RecordStore`) — the durable ground truth a KB is recovered
  from: canonical sentexes and justifications, keyed by integer handle, in a database
  an operator already runs.

  ## What a server buys, and what it does not

  It buys `COPY` (`copy-sentexes!` below, the fastest ingest path any records backend
  has), an operator's existing backup, PITR, replication, monitoring and access
  control, a store larger than one machine's disk, and a real `REPEATABLE READ`
  snapshot while something else is writing hard.

  It does **not** buy a shared KB.  Belief lives in the writing process's RAM — the
  JTMS and the taxonomy closures — so a second process connected to this same database
  does not see the first's beliefs, and its retraction sweep deletes records the first
  still believes.  That is the engine's single-writer contract, and a server does not
  weaken it by one clause (`docs/storage.md`, *The single-writer contract*).

  ## The shape

  Three tables, and the handle is the key throughout:

  * `vaelii_record (id, kind, frame, premise, strength)` — one row per record, `kind`
    splitting sentexes (0) from justifications (1), `frame` the **whole record**
    nippy-frozen so a fetch thaws back type-identical (a `LiteralSentex` stays a
    `LiteralSentex`).  A fetch passes the thawed record through the engine's codec
    (`codec/decode-sentex`, `codec/decode-justification`), which drops the fields the
    running engine's record types do not have.  `id` is **`bigint`**: handles are ints
    in the engine today, and a column type is the one place that decision becomes a
    migration on a table with 100M rows in it.
  * a sentex's **assumption strength** rides the `strength` column as the authoritative
    value and is `assoc`ed back onto the thawed record on read — so `mark-premise` is a
    one-row column update rather than a frame rewrite, and the column and the frame
    cannot drift because the column always wins.
  * `vaelii_record_provenance (id, prov)` and `vaelii_record_meta (k, v)`, the latter
    holding `high_water` — the largest handle ever allocated, so a delete of the max
    handle followed by a reopen still never reissues it.
  * `vaelii_record_stamp (store_id, writes)`, one row: a uuid minted when the row is
    first made, and a count a statement-level trigger on `vaelii_record` moves in the same
    transaction as every insert, update, delete and truncate — `COPY` included.  It is the
    fingerprint an index or reasoning image is validated against
    (`p/ImageFingerprinting`), so a KB can keep both images beside records on a server
    (`:pg-snapshot`): one row read at open says whether the records moved since the images
    were written, and the uuid says whether they are the same records at all.  A
    rolled-back statement does not move it.

  A put is **one statement**, not a transaction: a data-modifying CTE writes the record
  and moves `high_water` together, so a write costs one round trip rather than a
  `BEGIN`/two statements/`COMMIT` pair of four.  A delete pairs the record and its
  provenance the same way.

  ## The round trip, and what is done about it

  A point read is a network round trip where the disk store's is a page touch, and
  everything here is a response to that:

  * a **fetch LRU per kind** in front of every read, so a hot handle never reaches the
    server;
  * **premise strengths** held as one compressed handle set per strength, filled by the
    `premise-ids` walk itself — the walk already selects the column, and `recover` asks
    for every one of those strengths immediately afterwards, so the pair costs one scan
    instead of one scan plus a round trip per premise, at any premise count;
  * **enumeration streams** through a server-side cursor (`reduce-rows`): autocommit
    off and a set fetch size, which is what makes the driver hand back a portal instead
    of buffering the whole result.

  ## Connections

  Point the store at a db-spec (`{:dbtype \"postgresql\" :host … :dbname …}`, or
  `{:jdbcUrl \"jdbc:postgresql://…\"}`) and it builds and owns a HikariCP pool — one
  writer and N readers, by the contract above.  Hand it a `javax.sql.DataSource`
  instead and it borrows from that and closes nothing.  `:schema` puts the three tables
  in a schema of their own, so one database can hold several KBs and an operator can
  drop one with `DROP SCHEMA`.

  `fsync` is a no-op with a reason: durability here is the server's `fsync`, settled by
  its `synchronous_commit` and its WAL, and there is no client-side buffer for this
  store to force.  It registers its **close** with the engine's durability daemon all
  the same, so a JVM that exits without a `close!` still releases the pool.

  ## Boundary

  Apache-2.0, and an **adapter**: it implements the SSPL engine's
  `vaelii.impl.protocols/RecordStore` and is never depended on by it.  Core wires it in
  by a lazy `requiring-resolve`, so the engine never loads a JDBC driver unless a KB
  selects `:pg-memory` or `:pg-disk-log`."
  (:require [clojure.string :as str]
            [next.jdbc :as jdbc]
            [next.jdbc.connection :as conn]
            [next.jdbc.result-set :as rs]
            [taoensso.nippy :as nippy]
            [vaelii.impl.disk.codec :as codec]
            [vaelii.impl.disk.durability :as dur]
            [vaelii.impl.handle-set :as handle-set]
            [vaelii.impl.profile :as prof]
            [vaelii.impl.protocols :as p])
  (:import [com.zaxxer.hikari HikariConfig HikariDataSource]
           [java.io ByteArrayOutputStream Closeable DataOutputStream]
           [java.sql Connection PreparedStatement ResultSet]
           [java.util ArrayList Collections HashMap LinkedHashMap Map Map$Entry]
           [java.util.concurrent.locks ReentrantReadWriteLock]
           [javax.sql DataSource]
           [org.postgresql PGConnection]
           [org.postgresql.copy CopyIn CopyManager]))

;; ---- names --------------------------------------------------------------

(def ^:private kind-sentex 0)
(def ^:private kind-justification 1)

(def ^:private schema-pattern
  ;; The schema name is spliced into DDL and every statement, so it is held to a
  ;; plain identifier rather than quoted-and-hoped: a name that needs quoting is a
  ;; name this store will not make, and the refusal is at construction rather than in
  ;; a statement built from it.
  #"[A-Za-z_][A-Za-z0-9_]{0,62}")

(defn- check-schema!
  "Return `schema` (or nil), refusing anything that is not a bare SQL identifier."
  [schema]
  (when (some? schema)
    (let [s (name schema)]
      (when-not (re-matches schema-pattern s)
        (throw (ex-info (str "a :schema must be a bare SQL identifier (letters, digits and "
                             "underscores, not starting with a digit), got " (pr-str s))
                        {:type :bad-schema :schema s})))
      s)))

(defn- statements
  "Every statement this store runs, built once against the schema the tables live in.
  A map rather than a set of `def`s because the table names are not constant — a KB
  may put its three tables in a schema of its own — and building them once at
  construction is what keeps a name out of the per-op path."
  [schema]
  (let [q    (if schema (str schema \.) "")
        rec  (str q "vaelii_record")
        prov (str q "vaelii_record_provenance")
        meta (str q "vaelii_record_meta")
        fp   (str q "vaelii_record_stamp")]
    {:ddl-schema     (when schema (str "CREATE SCHEMA IF NOT EXISTS " schema))
     :ddl-record     (str "CREATE TABLE IF NOT EXISTS " rec " (
                             id       bigint   PRIMARY KEY,
                             kind     smallint NOT NULL,
                             frame    bytea    NOT NULL,
                             premise  boolean  NOT NULL DEFAULT false,
                             strength text)")
     ;; `premise-ids` is `WHERE kind = 0 AND premise`; a partial index keeps it
     ;; O(premises) on a store that is mostly non-premise derivations.
     :ddl-premise-ix (str "CREATE INDEX IF NOT EXISTS vaelii_record_premise ON " rec
                          " (id) WHERE premise")
     :ddl-prov       (str "CREATE TABLE IF NOT EXISTS " prov " (
                             id   bigint PRIMARY KEY,
                             prov bytea  NOT NULL)")
     :ddl-meta       (str "CREATE TABLE IF NOT EXISTS " meta " (
                             k text   PRIMARY KEY,
                             v bigint NOT NULL)")
     ;; the records fingerprint: one row, never truncated by `:clear` (the trigger
     ;; counts the truncate instead), so a cleared store's count moves past every image
     ;; of its previous content
     :ddl-fingerprint (str "CREATE TABLE IF NOT EXISTS " fp " (
                             one      boolean PRIMARY KEY DEFAULT true CHECK (one),
                             store_id uuid    NOT NULL DEFAULT gen_random_uuid(),
                             writes   bigint  NOT NULL DEFAULT 0)")
     :seed-fingerprint (str "INSERT INTO " fp " (one) VALUES (true) ON CONFLICT DO NOTHING")
     :ddl-bump-fn    (str "CREATE OR REPLACE FUNCTION " q "vaelii_record_bump()
                           RETURNS trigger LANGUAGE plpgsql AS $$
                           BEGIN
                             UPDATE " fp " SET writes = writes + 1;
                             RETURN NULL;
                           END $$")
     :ddl-bump       (str "CREATE OR REPLACE TRIGGER vaelii_record_bump"
                          " AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON " rec
                          " FOR EACH STATEMENT EXECUTE FUNCTION " q "vaelii_record_bump()")
     :fingerprint    (str "SELECT store_id::text AS store, writes FROM " fp)
     ;; one statement, one round trip: the record and the high-water mark move
     ;; together.  A re-put replaces the record and its authoritative strength and
     ;; leaves `premise` alone, so a re-put keeps a mark.
     :put-sentex     (str "WITH ins AS (
                             INSERT INTO " rec " AS r (id, kind, frame, premise, strength)
                             VALUES (?, " kind-sentex ", ?, false, ?)
                             ON CONFLICT (id) DO UPDATE
                               SET kind = excluded.kind, frame = excluded.frame,
                                   strength = excluded.strength)
                           INSERT INTO " meta " AS m (k, v) VALUES ('high_water', ?)
                           ON CONFLICT (k) DO UPDATE SET v = GREATEST(m.v, excluded.v)")
     :put-just       (str "WITH ins AS (
                             INSERT INTO " rec " AS r (id, kind, frame, premise, strength)
                             VALUES (?, " kind-justification ", ?, false, NULL)
                             ON CONFLICT (id) DO UPDATE
                               SET kind = excluded.kind, frame = excluded.frame)
                           INSERT INTO " meta " AS m (k, v) VALUES ('high_water', ?)
                           ON CONFLICT (k) DO UPDATE SET v = GREATEST(m.v, excluded.v)")
     :get-sentex     (str "SELECT frame, strength FROM " rec
                          " WHERE id = ? AND kind = " kind-sentex)
     ;; the prefetch hint's read: many handles at one round trip.  `= ANY(?)` over a
     ;; bigint array rather than a generated IN-list, so the statement text is constant
     ;; and the server's prepared plan is reused whatever the chunk holds.
     :get-many       (str "SELECT id, frame, strength FROM " rec
                          " WHERE id = ANY(?) AND kind = " kind-sentex)
     :get-many-just  (str "SELECT id, frame FROM " rec
                          " WHERE id = ANY(?) AND kind = " kind-justification)
     :get-just       (str "SELECT frame FROM " rec
                          " WHERE id = ? AND kind = " kind-justification)
     ;; the record and its provenance are torn down together, in one statement
     :del-sentex     (str "WITH del AS (DELETE FROM " rec
                          " WHERE id = ? AND kind = " kind-sentex ")
                           DELETE FROM " prov " WHERE id = ?")
     :del-just       (str "WITH del AS (DELETE FROM " rec
                          " WHERE id = ? AND kind = " kind-justification ")
                           DELETE FROM " prov " WHERE id = ?")
     :put-prov       (str "INSERT INTO " prov " AS p (id, prov) VALUES (?, ?)
                           ON CONFLICT (id) DO UPDATE SET prov = excluded.prov")
     :get-prov       (str "SELECT prov FROM " prov " WHERE id = ?")
     :del-prov       (str "DELETE FROM " prov " WHERE id = ?")
     :sentex-ids     (str "SELECT id FROM " rec " WHERE kind = " kind-sentex)
     ;; the `Tallying` half: the questions a caller asks the handle set that do not need
     ;; the set.  A count is an index-only scan of one column; a sample stops at the first
     ;; row.  Both replace a walk that puts every handle in the store on the wire.
     :sentex-count   (str "SELECT count(*) FROM " rec " WHERE kind = " kind-sentex)
     :just-count     (str "SELECT count(*) FROM " rec " WHERE kind = " kind-justification)
     :a-sentex       (str "SELECT id FROM " rec " WHERE kind = " kind-sentex " LIMIT 1")
     :a-just         (str "SELECT id FROM " rec " WHERE kind = " kind-justification " LIMIT 1")
     :a-premise      (str "SELECT id FROM " rec " WHERE kind = " kind-sentex
                          " AND premise LIMIT 1")
     :just-ids       (str "SELECT id FROM " rec " WHERE kind = " kind-justification)
     ;; the strength rides along: the walk that answers `premise-ids` is the walk that
     ;; fills the strength cache `recover` reads a moment later
     :premise-ids    (str "SELECT id, strength FROM " rec
                          " WHERE kind = " kind-sentex " AND premise")
     :mark-premise   (str "UPDATE " rec " SET premise = true, strength = ?"
                          " WHERE id = ? AND kind = " kind-sentex)
     :unmark-premise (str "UPDATE " rec " SET premise = false, strength = NULL"
                          " WHERE id = ? AND kind = " kind-sentex)
     :strength-of    (str "SELECT strength FROM " rec
                          " WHERE id = ? AND kind = " kind-sentex)
     :clear          (str "TRUNCATE " rec ", " prov ", " meta)
     :high-water     (str "SELECT v FROM " meta " WHERE k = 'high_water'")
     :max-id         (str "SELECT MAX(id) AS m FROM " rec)
     :bump-water     (str "INSERT INTO " meta " AS m (k, v) VALUES ('high_water', ?)
                           ON CONFLICT (k) DO UPDATE SET v = GREATEST(m.v, excluded.v)")
     :copy           (str "COPY " rec " (id, kind, frame, premise, strength)"
                          " FROM STDIN (FORMAT binary)")
     ;; the two table names themselves, for the statements whose text depends on how many
     ;; rows a chunk holds (`mark-premise-sql`, `put-provenance-sql`) and so cannot be
     ;; built once here
     :rec-table      rec
     :prov-table     prov}))

;; ---- helpers ------------------------------------------------------------

(def ^:private lower-maps {:builder-fn rs/as-unqualified-lower-maps})

(defn- lru
  "A synchronized, access-ordered LRU bounded at `cap` entries — the fetch cache in
  front of a read that would otherwise cost a round trip."
  ^Map [^long cap]
  (Collections/synchronizedMap
   (proxy [LinkedHashMap] [16 (float 0.75) true]
     (removeEldestEntry [_] (> (.size ^LinkedHashMap this) cap)))))

(defn- id-ok?
  "The record protocol is asked for non-integer and negative handles — an informant
  keyword lands here — and must answer nil rather than throw.  Every fetch checks
  this first."
  [id]
  (and (integer? id) (not (neg? ^long id))))

;; Every cache is keyed by the **boxed long**, never by whatever integer type the
;; caller happened to hold: `Integer 5` and `Long 5` are different map keys, so a
;; delete that missed the cache would leave the deleted record answering reads.
(defn- ckey ^Long [id] (Long/valueOf (long id)))

;; ---- the cache guard ----------------------------------------------------
;;
;; Every read here is a **read-then-fill**: miss the cache, go to the server, install what
;; came back.  A write that lands between the server read and the install is undone by it
;; — the fetch reinstalls a record `delete-sentex!` has just purged, and it stays installed
;; until the LRU evicts it, so a retracted fact goes on answering a reader's queries.  The
;; engine supports exactly that arrangement (`docs/storage.md`, one writer and a reader
;; beside it), so the race window is reachable rather than theoretical.
;;
;; A **read/write** lock rather than the sibling stores' exclusive one, because this store
;; is pooled and concurrent reads are the reason it is: fills share the read side and only
;; a mutation takes the write side.  A cache **hit** takes no lock at all — the fill is the
;; only part that can be undone.
(defmacro ^:private filling
  "Run `body` — a cache miss's server read and install — under the fill (read) side."
  [lk & body]
  `(let [^ReentrantReadWriteLock l# ~lk
         r# (.readLock l#)]
     (.lock r#)
     (try ~@body (finally (.unlock r#)))))

(defmacro ^:private mutating
  "Run `body` — a write and the cache invalidation that belongs to it — under the write
  side, so no in-flight fill can reinstate what it removes."
  [lk & body]
  `(let [^ReentrantReadWriteLock l# ~lk
         w# (.writeLock l#)]
     (.lock w#)
     (try ~@body (finally (.unlock w#)))))

(defn- str->strength [s] (some-> s keyword))
(defn- strength->str [k] (some-> k name))

;; ---- the premise strengths ----------------------------------------------
;;
;; `premise-strength` answers from a map of strength to a `vaelii.impl.handle-set` live
;; handle set: a handle in the `:monotonic` set has strength `:monotonic`.  A strength is
;; one of a few keywords, so the map holds a few entries, and each set holds its premises
;; at a fraction of a byte a handle where a map from handle to strength holds a boxed entry
;; apiece.  A handle in no set answers from the `strength` column.
;;
;; A live handle set synchronizes nothing, so each function below holds the map's monitor.
;; The writes that move a strength call them under `mutating`, and the walk installs under
;; `filling`, so the cache guard above orders a walk's install against every mutation.

(defn- strength-set!
  "The live handle set for strength `st` in `sets`, added empty when absent.  The caller
  holds the monitor of `sets`, or owns `sets` outright."
  [^Map sets st]
  (or (.get sets st)
      (let [s (handle-set/live-handle-set)] (.put sets st s) s)))

(defn- drop-strength!
  "Remove handle `id` from every strength set."
  [^Map sets id]
  (locking sets
    (doseq [s (.values sets)] (handle-set/live-remove! s id))))

(defn- set-strength!
  "Record strength `st` for handle `id`, removing `id` from every other strength's set."
  [^Map sets id st]
  (locking sets
    (doseq [s (.values sets)] (handle-set/live-remove! s id))
    (handle-set/live-add! (strength-set! sets st) id)))

(defn- strength-in
  "The strength whose set holds handle `id`, or nil when no set does."
  [^Map sets id]
  (locking sets
    (reduce (fn [_ ^Map$Entry e]
              (when (handle-set/live-has? (.getValue e) id) (reduced (.getKey e))))
            nil (.entrySet sets))))

(defn- replace-strengths!
  "Replace every strength set in `sets` with the sets in `fresh`."
  [^Map sets ^Map fresh]
  (locking sets
    (.clear sets)
    (.putAll sets fresh)))

(defn- clear-strengths! [^Map sets] (locking sets (.clear sets)))

;; ---- streaming enumeration ----------------------------------------------

(defn reduce-rows
  "Reduce `rf` over the rows `sql` selects, streaming them through a **server-side
  cursor** rather than buffering the result: the walk borrows a connection, turns
  autocommit off and sets a fetch size, which together are what make the driver hand
  back a portal.  Without both, `SELECT id FROM vaelii_record` on a large store reads
  every row into the driver before the first one is visible.

  `rf` is an ordinary reducing fn `(rf acc ^ResultSet rs)`, called with the cursor
  positioned on each row; wrapping the accumulator in `reduced` stops the walk and
  closes the cursor without draining it.  The transaction is rolled back on the way
  out — every walk here is a read — and the connection goes back to the pool."
  [store sql params rf init]
  (let [^DataSource ds (:ds store)
        ^long fetch    (:fetch-size store)
        ^Connection c  (.getConnection ds)
        restore        (.getAutoCommit c)]
    (try
      (.setAutoCommit c false)
      (with-open [^PreparedStatement ps (.prepareStatement c ^String sql)]
        (.setFetchSize ps (int fetch))
        (dotimes [i (count params)]
          (.setObject ps (int (inc i)) (nth params i)))
        (with-open [^ResultSet rs (.executeQuery ps)]
          (loop [acc init]
            (if (reduced? acc)
              @acc
              (if (.next rs) (recur (rf acc rs)) acc)))))
      (finally
        (try (.rollback c) (catch Exception _ nil))
        (try (.setAutoCommit c restore) (catch Exception _ nil))
        (.close c)))))

(defn- id-handle-set
  "Every handle `sql` selects, as a `vaelii.impl.handle-set` set.

  A set and not a stream, because the protocol says so: `recover` tests membership in the
  live handles for every premise it roots and for every antecedent of every
  justification (`contains?`), which no lazy sequence answers.  What streaming buys is
  the *walk* — the driver never holds the whole result.

  **Compressed, because this is the store the shape costs something on.**  A
  `PersistentHashSet<Long>` retains 48–75 bytes a handle, so the set of a 100M-record
  store is 4.5–7.0 GB of the caller's heap before a record is fetched; handles arrive
  from the cursor in a near-contiguous run, which a bitmap holds in a few run containers
  at 0.13–0.26 bytes apiece — and answers `contains?` faster than the hash set, not
  slower (core's `docs/storage.md`, the enumeration contract)."
  [store sql]
  (let [[add! finish] (handle-set/collector)]
    (reduce-rows store sql []
                 (fn [acc ^ResultSet rs] (add! (.getLong rs 1)) acc)
                 nil)
    (finish)))

;; ---- the records fingerprint --------------------------------------------

(defn- read-fingerprint
  "`{:store uuid-string :writes n}`, read fresh off the fingerprint row.  Never cached: the
  fingerprint vouches for an image against what the server holds now, and a copy held
  here would vouch for one written before a write this process never saw."
  [ds sql]
  (let [{:keys [store writes]} (jdbc/execute-one! ds [(:fingerprint sql)] lower-maps)]
    {:store store :writes (long writes)}))

;; ---- the store ----------------------------------------------------------

(defrecord PgRecordStore [ds sql counter ^Map sx-cache ^Map j-cache
                          ^Map strengths fetch-size cache-cap
                          prefetch prefetch-min own-pool? dur-id lock]
  p/RecordStore

  (next-id [_] (long (swap! counter inc)))

  (put-sentex [this sentex]
    (let [id     (long (or (:id sentex) (p/next-id this)))
          sentex (assoc sentex :id id)]
      (swap! counter max id)
      (mutating lock
                (jdbc/execute-one! ds [(:put-sentex sql) id (nippy/freeze sentex)
                                       (strength->str (:strength sentex)) id])
                (.put sx-cache (ckey id) sentex)
                ;; the put rewrote the authoritative `strength` column and left `premise`
                ;; alone, so a held strength for this handle is now the old one
                (drop-strength! strengths id))
      id))

  (get-sentex [_ id]
    ;; Tallied on the protocol method and not on the query below it, so the number
    ;; counts what a *caller* asked for rather than what this backend did about it —
    ;; the same event the RAM and disk stores count, which is what makes the tallies
    ;; comparable across backends.  It is therefore **not** the round-trip count: the
    ;; LRU stands between the two, and the gap between them is what it is worth.
    (prof/record-fetch :sentex)
    (when (id-ok? id)
      (let [k (ckey id)]
        (or (.get sx-cache k)
            (filling lock
                     (when-let [row (jdbc/execute-one! ds [(:get-sentex sql) (long id)] lower-maps)]
                       ;; the column is authoritative — assoc it back, so a `mark-premise`
                       ;; that touched only the column is reflected without a frame rewrite
                       (let [sx (assoc (codec/decode-sentex (nippy/thaw ^bytes (:frame row)))
                                       :strength (str->strength (:strength row)))]
                         (.put sx-cache k sx)
                         sx)))))))

  (delete-sentex! [_ id]
    (when (id-ok? id)
      (let [id (long id)]
        (mutating lock
                  (jdbc/execute-one! ds [(:del-sentex sql) id id])
                  (.remove sx-cache (ckey id))
                  (drop-strength! strengths id))))
    nil)

  (put-justification [this justification]
    (let [id (long (or (:id justification) (p/next-id this)))
          justification (assoc justification :id id)]
      (swap! counter max id)
      (mutating lock
                (jdbc/execute-one! ds [(:put-just sql) id (nippy/freeze justification) id])
                (.put j-cache (ckey id) justification))
      id))

  (get-justification [_ id]
    (prof/record-fetch :justification)
    (when (id-ok? id)
      (let [k (ckey id)]
        (or (.get j-cache k)
            (filling lock
                     (when-let [row (jdbc/execute-one! ds [(:get-just sql) (long id)] lower-maps)]
                       (let [d (codec/decode-justification (nippy/thaw ^bytes (:frame row)))]
                         (.put j-cache k d)
                         d)))))))

  (delete-justification! [_ id]
    (when (id-ok? id)
      (let [id (long id)]
        (mutating lock
                  (jdbc/execute-one! ds [(:del-just sql) id id])
                  (.remove j-cache (ckey id)))))
    nil)

  ;; Guarded like the fetches: `id-ok?`'s contract is that a handle this store could
  ;; never have issued reads as nil rather than throwing, and the memory store makes
  ;; these four ops no-ops for one.  Unguarded, `(long id)` turns an informant keyword
  ;; into a ClassCastException from an entry point that is supposed to be quiet.
  (put-provenance [_ id prov]
    (when (id-ok? id)
      (jdbc/execute-one! ds [(:put-prov sql) (long id) (nippy/freeze prov)]))
    prov)

  (get-provenance [_ id]
    (prof/record-fetch :provenance)
    (when (id-ok? id)
      (when-let [row (jdbc/execute-one! ds [(:get-prov sql) (long id)] lower-maps)]
        (nippy/thaw ^bytes (:prov row)))))

  (delete-provenance! [_ id]
    (when (id-ok? id)
      (jdbc/execute-one! ds [(:del-prov sql) (long id)]))
    nil)

  (sentex-ids [this] (id-handle-set this (:sentex-ids sql)))

  (justification-ids [this] (id-handle-set this (:just-ids sql)))

  (mark-premise [_ id strength]
    ;; the UPDATE's WHERE is the guard against a phantom premise: it touches nothing
    ;; when the sentex is absent.  Column-only, so no frame rewrite — evict the cached
    ;; record so the next fetch reflects the new strength.
    (when (id-ok? id)
      (let [strength (or strength :default)
            k        (ckey id)]
        (mutating lock
                  (let [n (:next.jdbc/update-count
                           (jdbc/execute-one! ds [(:mark-premise sql)
                                                  (strength->str strength) (long id)]))]
                    (.remove sx-cache k)
                    ;; **Only what the UPDATE actually marked.**  Its WHERE is the guard
                    ;; against a phantom premise — it touches nothing when the sentex is
                    ;; absent — and a strength recorded outside that test would answer
                    ;; `premise-strength` for a handle `premise-ids` does not name.
                    (when (pos? (long (or n 0)))
                      (set-strength! strengths id strength))))))
    nil)

  (unmark-premise! [_ id]
    (when (id-ok? id)
      (mutating lock
                (jdbc/execute-one! ds [(:unmark-premise sql) (long id)])
                (.remove sx-cache (ckey id))
                (drop-strength! strengths id)))
    nil)

  (premise-ids [this]
    ;; the strengths ride the same walk.  `recover` calls `premise-strength` for every
    ;; handle this returns, and a round trip apiece is the difference between a scan
    ;; and an hour: filling the strength sets here costs the column the query already reads.
    ;;
    ;; The walk builds fresh sets and installs them whole, replacing what `strengths` held:
    ;; the cursor reads every premise, so the fresh sets answer for each one.  The whole walk
    ;; and the install hold the fill (read) side: a mutation landing between the cursor's
    ;; read and the install would be undone by it — the reinstatement `mutating` exists to
    ;; exclude.  Readers share the side; only a mutation waits, and the usual caller is the
    ;; one writer (`recover`).
    (let [[add! finish] (handle-set/collector)
          fresh         (HashMap.)]
      (filling lock
               (reduce-rows this (:premise-ids sql) []
                            (fn [acc ^ResultSet rs]
                              (let [id (.getLong rs 1)
                                    ;; a marked row whose strength column is NULL is
                                    ;; reachable (a re-put carrying no `:strength` rewrites
                                    ;; the column and leaves the mark), and the reference
                                    ;; stores answer `:default` for it
                                    st (or (str->strength (.getString rs 2)) :default)]
                                (handle-set/live-add! (strength-set! fresh st) id)
                                (add! id)
                                acc))
                            nil)
               (doseq [s (.values fresh)] (handle-set/live-optimize! s))
               (replace-strengths! strengths fresh))
      (finish)))

  (premise-strength [_ id]
    (or (when (id-ok? id)
          (or (strength-in strengths id)
              (str->strength
               (:strength (jdbc/execute-one! ds [(:strength-of sql) (long id)] lower-maps)))))
        :default))

  (clear-records! [_]
    (mutating lock
              (jdbc/execute-one! ds [(:clear sql)])
              (reset! counter 0)
              (.clear sx-cache)
              (.clear j-cache)
              (clear-strengths! strengths))
    nil)

  p/ImageFingerprinting
  ;; one fingerprint for both images: the trigger counts every statement on the record
  ;; table, justifications included, so it moves whenever either image's records do
  (index-fingerprint [_] (read-fingerprint ds sql))
  (belief-fingerprint [_] (read-fingerprint ds sql))

  p/Tallying
  ;; **The whole reason the capability exists.**  `open-kb` asks how many records this
  ;; store holds (the durable index's coverage check) and whether it holds any at all (the
  ;; recovery branch), and `import` asks the first again to report what it loaded.  Every
  ;; one of those spelled as `(count (sentex-ids …))` is this table on the wire: 100M
  ;; handles streamed through a cursor and a set built out of them, to answer with one
  ;; number.  Here they are one row each — the counts an index-only scan over `kind`, the
  ;; samples a `LIMIT 1` that stops at the first.
  ;;
  ;; No lock: these are reads, and they touch no cache.  A count taken while the writer
  ;; writes is the count at some instant during the call, which is what the enumeration
  ;; they replace answered too.
  (sentex-tally [_]
    (long (or (:count (jdbc/execute-one! ds [(:sentex-count sql)] lower-maps)) 0)))
  (justification-tally [_]
    (long (or (:count (jdbc/execute-one! ds [(:just-count sql)] lower-maps)) 0)))
  (a-sentex-id [_]
    (:id (jdbc/execute-one! ds [(:a-sentex sql)] lower-maps)))
  (a-justification-id [_]
    (:id (jdbc/execute-one! ds [(:a-just sql)] lower-maps)))
  (a-premise-id [_]
    (:id (jdbc/execute-one! ds [(:a-premise sql)] lower-maps)))

  p/Prefetching
  (prefetch-justifications! [_ ids]
    ;; `recover` fetches every stored justification, one handle at a time — the single
    ;; largest run of round trips a networked store sees, and the one the open pays on
    ;; every start.  Same rule as the sentex half: only what the cache does not hold, and
    ;; only when there is enough of it to beat the point reads.
    (when (and prefetch (pos? (long cache-cap)))
      (let [missing (into [] (comp (map ckey)
                                   (remove #(.containsKey j-cache %))
                                   (take (long cache-cap)))
                          ids)]
        (when (>= (count missing) (long prefetch-min))
          (filling lock
                   (with-open [^Connection c (.getConnection ^DataSource ds)]
                     (let [arr (.createArrayOf c "bigint" (into-array Long missing))]
                       (with-open [^PreparedStatement ps (.prepareStatement c ^String (:get-many-just sql))]
                         (.setArray ps 1 arr)
                         (with-open [^ResultSet rs (.executeQuery ps)]
                           (while (.next rs)
                             (.put j-cache (ckey (.getLong rs 1))
                                   (codec/decode-justification (nippy/thaw ^bytes (.getBytes rs 2)))))))))))))
    nil)

  (prefetch-sentexes! [_ ids]
    ;; **The cache is the oracle.**  A hint is worth acting on exactly when these handles
    ;; are not already held, and that is a `containsKey` apiece in RAM rather than a
    ;; guess from a hit-rate average — so a KB whose corpus fits the cache pays the scan
    ;; and issues no query at all, and one whose working set does not gets a round trip
    ;; instead of `(count missing)` of them.  `containsKey` is not an access on an
    ;; access-ordered LinkedHashMap, so looking does not disturb the eviction order.
    ;;
    ;; Capped at the cache's own capacity: a chunk larger than the cache would evict what
    ;; it just installed and the fetches would miss anyway.
    (when (and prefetch (pos? (long cache-cap)))
      (let [missing (into [] (comp (map ckey)
                                   (remove #(.containsKey sx-cache %))
                                   (take (long cache-cap)))
                          ids)]
        (when (>= (count missing) (long prefetch-min))
          (filling lock
                   (with-open [^Connection c (.getConnection ^DataSource ds)]
                     (let [arr (.createArrayOf c "bigint" (into-array Long missing))]
                       (with-open [^PreparedStatement ps (.prepareStatement c ^String (:get-many sql))]
                         (.setArray ps 1 arr)
                         (with-open [^ResultSet rs (.executeQuery ps)]
                           (while (.next rs)
                             (let [id (ckey (.getLong rs 1))
                                   sx (assoc (codec/decode-sentex (nippy/thaw ^bytes (.getBytes rs 2)))
                                             :strength (str->strength (.getString rs 3)))]
                               (.put sx-cache id sx)))))))))))
    nil)

  java.io.Closeable
  (close [_]
    (dur/deregister! @dur-id)
    (.clear sx-cache)
    (.clear j-cache)
    (clear-strengths! strengths)
    ;; a pool this store built is this store's to close; a DataSource handed in
    ;; belongs to whoever made it
    (when own-pool? (.close ^HikariDataSource ds))
    nil))

;; ---- COPY: the bulk ingest path -----------------------------------------

(def ^:private copy-signature
  (byte-array (map unchecked-byte [0x50 0x47 0x43 0x4F 0x50 0x59 0x0A 0xFF 0x0D 0x0A 0x00])))

(defn- write-copy-header! [^DataOutputStream out]
  (let [^bytes sig copy-signature]
    (.write out sig 0 (alength sig)))
  (.writeInt out 0)                     ; flags
  (.writeInt out 0))                    ; header extension length

(defn- write-copy-row!
  "One binary-COPY tuple for `vaelii_record`: five fields, each a length and its
  bytes, `-1` for NULL."
  ;; no primitive hints on the numbers: a fn taking primitives is capped at four args,
  ;; and a tuple is five fields wide.  They are coerced at the write instead.
  [^DataOutputStream out id kind ^bytes frame ^String strength premise?]
  (.writeShort out 5)
  (.writeInt out 8)   (.writeLong out (long id))
  (.writeInt out 2)   (.writeShort out (int kind))
  (.writeInt out (alength frame)) (.write out frame 0 (alength frame))
  (.writeInt out 1)   (.writeByte out (if premise? 1 0))
  (if strength
    (let [b (.getBytes strength "UTF-8")]
      (.writeInt out (alength b))
      (.write out b 0 (alength b)))
    (.writeInt out -1)))

(defn- bump-high-water!
  "Move the handle counter and the persisted `high_water` past what a COPY just landed —
  after its connection is back in the pool, never while it is held."
  [store n top]
  (when (pos? (long n))
    (swap! (:counter store) max (long top))
    (jdbc/execute-one! (:ds store) [(:bump-water (:sql store)) (long top)]))
  nil)

(def ^:private annotate-chunk
  "Rows per statement in a bulk annotate.  Postgres caps a statement at 65,535 bound
  parameters and each row here binds two, so this is a fraction of the ceiling and the
  statement stays inside one plan."
  1000)

(defn- mark-premise-sql
  "`UPDATE … FROM (VALUES …)` marking `n` handles in one statement, `RETURNING` the ids it
  actually touched — which is the same guard the one-row form has (a handle with no sentex
  matches nothing) and is what says which cache entries may be filled."
  [sql ^long n]
  (str "UPDATE " (:rec-table sql) " AS r SET premise = true, strength = v.s"
       " FROM (VALUES "
       (str/join ", " (repeat n "(?::bigint, ?::text)"))
       ") AS v(id, s) WHERE r.id = v.id AND r.kind = " kind-sentex
       " RETURNING r.id"))

(defn- put-provenance-sql
  "A multi-row upsert of `n` provenance maps — one statement, the same `ON CONFLICT` the
  one-row form has."
  [sql ^long n]
  (str "INSERT INTO " (:prov-table sql) " AS p (id, prov) VALUES "
       (str/join ", " (repeat n "(?, ?)"))
       " ON CONFLICT (id) DO UPDATE SET prov = excluded.prov"))

(def ^:private default-copy-batch
  "Rows buffered before one write to the copy stream.  The unit is a byte buffer built and
  handed to the driver, so it trades peak heap for syscalls and neither end is delicate."
  10000)

(defn- copy-sink
  "An open `COPY … FROM STDIN BINARY` — the connection and the copy stream held for the
  sink's lifetime, `batch` rows buffered between writes into it.  `close` flushes the
  remainder, ends the copy and returns the connection to the pool.

  **All or nothing, unlike a stream of puts.**  Every row written here belongs to the one
  `COPY` statement, so it lands when `close` ends the copy and not before: a sink that
  throws part way through leaves the store as it found it, where a loop of `put-sentex`
  leaves everything up to the failure.  That is the better half of the trade and the one
  worth knowing about, since it makes a failed bulk import need no `clear!`.

  COPY has no `ON CONFLICT`, so this is a load into handles the store does not already
  hold — a fresh store, or a dump whose handles are being preserved.  A duplicate handle
  raises rather than overwriting, so a bulk load never silently replaces a record the
  caller did not know was there.

  Nothing written here enters this store's read caches.  A bulk load is a stream nobody
  is reading back, and filling a 65k-entry LRU with the tail of a 30M-record corpus
  evicts what a later query wants for no gain at all."
  [store kind strength-of premise? batch]
  (when-not (pos? (long batch))
    (throw (ex-info (str "a COPY :batch must be a positive number of rows, got "
                         (pr-str batch))
                    {:type :bad-batch :batch batch})))
  (let [^DataSource ds (:ds store)
        sql            (:sql store)
        ^Connection c  (.getConnection ds)]
    (try
      (let [mgr        (.getCopyAPI ^PGConnection (.unwrap c PGConnection))
            ^CopyIn in (.copyIn ^CopyManager mgr ^String (:copy sql))
            pending    (ArrayList.)
            n          (volatile! 0)
            top        (volatile! 0)
            flush!     (fn []
                         (when-not (.isEmpty pending)
                           (let [bos (ByteArrayOutputStream. (* 1024 (.size pending)))
                                 out (DataOutputStream. bos)]
                             (when (zero? (long @n)) (write-copy-header! out))
                             (doseq [r pending]
                               (let [id (long (:id r))
                                     st (strength-of r)]
                                 (vswap! top max id)
                                 (vswap! n inc)
                                 (write-copy-row! out id kind (nippy/freeze r) st
                                                  (and premise? (some? st)))))
                             (.flush out)
                             (let [b (.toByteArray bos)]
                               (.writeToCopy in b 0 (alength b)))
                             (.clear pending))))]
        (reify
          p/RecordSink
          (write-record! [_ rec]
            (let [id  (long (or (:id rec) (p/next-id store)))
                  rec (assoc rec :id id)]
              (swap! (:counter store) max id)
              (.add pending rec)
              (when (>= (.size pending) (long batch)) (flush!))
              id))

          Closeable
          (close [_]
            (try
              (flush!)
              ;; the trailer, and the header when the stream was empty
              (let [bos (ByteArrayOutputStream. 32)
                    out (DataOutputStream. bos)]
                (when (zero? (long @n)) (write-copy-header! out))
                (.writeShort out -1)
                (.flush out)
                (let [b (.toByteArray bos)]
                  (.writeToCopy in b 0 (alength b))))
              (.endCopy in)
              (catch Throwable t
                (try (when (.isActive in) (.cancelCopy in)) (catch Exception _ nil))
                (throw t))
              (finally (.close c)))
            ;; the high-water write is deliberately NOT inside that `finally`: it runs on
            ;; `ds`, which borrows a second connection while this one is still held — a
            ;; self-deadlock at `:pool-size 1`, where the COPY has already committed and
            ;; the caller is told it failed.
            (bump-high-water! store @n @top)
            nil)))
      (catch Throwable t (.close c) (throw t)))))

(defn copy-sentexes!
  "Bulk-load `sentexes` — each carrying its `:id` — through `COPY … FROM STDIN
  BINARY`, returning how many rows landed.  This is the ingest path a server has and
  the others do not, and it is a load rather than an upsert: a handle the store already
  holds raises rather than being overwritten.

  **A record's `:strength` marks it as a premise here**, where `put-sentex` leaves the
  mark to `mark-premise`.  The two entry points are asked different questions: the engine
  stores a fact and marks it at the choke point one line later, while a bulk load has no
  such line — the strength a dump's record carries *is* what a later `recover` reads as
  its assumption, and re-asking for it a handle at a time is a round trip per record.
  `{:premises? false}` loads the strengths without the marks, for records that are
  derivations rather than assumptions.  `:batch` rows per buffer (default 10000).

  This is the protocol's own public entry point, for an application holding a corpus already.  The
  **engine** reaches the same path without knowing this namespace exists, through
  `protocols/BulkLoading` — which is what `import!` writes its records through."
  ([store sentexes] (copy-sentexes! store sentexes {}))
  ([store sentexes {:keys [batch premises?] :or {batch default-copy-batch premises? true}}]
   (let [n (volatile! 0)]
     (with-open [^Closeable sink (copy-sink store kind-sentex
                                            #(strength->str (:strength %)) premises? batch)]
       (doseq [sx sentexes]
         (p/write-record! sink sx)
         (vswap! n inc)))
     @n)))

(defn copy-justifications!
  "Bulk-load `justifications` — each carrying its `:id` — the same way
  `copy-sentexes!` loads sentexes.  A justification has no strength column."
  ([store justifications] (copy-justifications! store justifications {}))
  ([store justifications {:keys [batch] :or {batch default-copy-batch}}]
   (let [n (volatile! 0)]
     (with-open [^Closeable sink (copy-sink store kind-justification
                                            (constantly nil) false batch)]
       (doseq [j justifications]
         (p/write-record! sink j)
         (vswap! n inc)))
     @n)))

(defn- mark-premises-in-chunks!
  "`id->strength` marked a chunk at a time, one statement per chunk.

  The `RETURNING` is not decoration: the one-row form reads its update count to decide
  whether a strength may be recorded — a handle with no sentex matches nothing and
  must not answer `premise-strength` for a handle `premise-ids` does not name — and over a
  batch the count alone cannot say *which* rows matched, so the statement says.

  In id order, so a chunk's rows share heap pages and consecutive chunks walk the table
  forward.  `id->strength` is a hash map, and in its order each row of a chunk is a page
  of its own: on a table larger than `shared_buffers`, a read from disk per row."
  [store id->strength]
  (let [^DataSource ds (:ds store)
        sql            (:sql store)
        ^Map sx-cache  (:sx-cache store)
        strengths      (:strengths store)]
    (doseq [chunk (partition-all annotate-chunk
                                 (sort-by key (filter (fn [[id _]] (id-ok? id)) id->strength)))]
      (let [rows   (vec chunk)
            params (into [] (mapcat (fn [[id st]] [(long id) (strength->str (or st :default))]))
                         rows)]
        (mutating (:lock store)
                  (let [marked (into #{}
                                     (map :id)
                                     (jdbc/execute! ds (into [(mark-premise-sql sql (count rows))]
                                                             params)
                                                    lower-maps))]
                    (doseq [[id st] rows]
                      (.remove sx-cache (ckey id))
                      (when (contains? marked (long id))
                        (set-strength! strengths id (or st :default)))))))))
  nil)

(defn- put-provenance-in-chunks!
  "`entries` upserted a chunk at a time, one statement per chunk."
  [store entries]
  (let [^DataSource ds (:ds store)
        sql            (:sql store)]
    (doseq [chunk (partition-all annotate-chunk (filter (fn [[id _]] (id-ok? id)) entries))]
      (let [rows   (vec chunk)
            params (into [] (mapcat (fn [[id prov]] [(long id) (nippy/freeze prov)])) rows)]
        (mutating (:lock store)
                  (jdbc/execute-one! ds (into [(put-provenance-sql sql (count rows))] params))))))
  nil)

(extend-protocol p/BulkAnnotating
  PgRecordStore
  (mark-premise-batch [store id->strength] (mark-premises-in-chunks! store id->strength))
  (put-provenance-batch [store entries] (put-provenance-in-chunks! store entries)))

(extend-protocol p/BulkLoading
  PgRecordStore
  (open-sentex-sink [store {:keys [batch premises?] :or {batch default-copy-batch
                                                         premises? true}}]
    (copy-sink store kind-sentex #(strength->str (:strength %)) premises? batch))
  (open-justification-sink [store {:keys [batch] :or {batch default-copy-batch}}]
    (copy-sink store kind-justification (constantly nil) false batch)))

;; ---- construction -------------------------------------------------------

(def ^:private default-cache-capacity 65536)
(def ^:private default-fetch-size 5000)
(def ^:private default-pool-size 8)

;; How many *uncached* handles in a hint make one query cheaper than the point reads it
;; replaces.  One round trip against N, so the break-even is low; above 1 only because a
;; hint of one or two saves nothing worth the array and the extra statement.
(def ^:private default-prefetch-min 4)

(def store-keys
  "The keys `pg-record-store` reads itself.  Everything else in the spec is the
  database's — `:dbtype`, `:host`, `:dbname`, `:jdbcUrl`, credentials — and is handed
  to the driver untouched, so this is the set that must be stripped before it is."
  #{:schema :cache-capacity :premise-cache-capacity :fetch-size :pool-size
    :prefetch :prefetch-min})

(defn ensure-schema!
  "Create the record, provenance, meta and fingerprint tables (and the premise index and
  the trigger that moves the fingerprint), and the schema itself when one is named.
  Idempotent; runs before any op, so a store made before the fingerprint table existed
  gains it at its next open, counting from zero — it has no image to be checked against
  yet."
  [ds sql]
  (when-let [s (:ddl-schema sql)] (jdbc/execute-one! ds [s]))
  (jdbc/execute-one! ds [(:ddl-record sql)])
  (jdbc/execute-one! ds [(:ddl-premise-ix sql)])
  (jdbc/execute-one! ds [(:ddl-prov sql)])
  (jdbc/execute-one! ds [(:ddl-meta sql)])
  (jdbc/execute-one! ds [(:ddl-fingerprint sql)])
  (jdbc/execute-one! ds [(:seed-fingerprint sql)])
  (jdbc/execute-one! ds [(:ddl-bump-fn sql)])
  (jdbc/execute-one! ds [(:ddl-bump sql)]))

(defn- ->pool
  "A HikariCP pool over `spec`, built directly rather than reflectively so every call
  here is hinted."
  ^HikariDataSource [spec ^long pool-size]
  (let [cfg (HikariConfig.)
        url (or (:jdbcUrl spec) (conn/jdbc-url (dissoc spec :user :username :password)))]
    (.setJdbcUrl cfg ^String url)
    (when-let [u (or (:username spec) (:user spec))] (.setUsername cfg ^String u))
    (when-let [pw (:password spec)] (.setPassword cfg ^String pw))
    (.setMaximumPoolSize cfg (int pool-size))
    (.setPoolName cfg "vaelii-records")
    (HikariDataSource. cfg)))

(defn- load-high-water
  "The counter's opening value: the larger of the persisted `high_water` and the live
  maximum handle, so a store that lost its last `high_water` write to a crash still
  never reissues a live handle, and one that deleted its max handle still never
  reissues *that*."
  ^long [ds sql]
  (let [hw   (:v (jdbc/execute-one! ds [(:high-water sql)] lower-maps))
        live (:m (jdbc/execute-one! ds [(:max-id sql)] lower-maps))]
    (max (long (or hw 0)) (long (or live 0)))))

(defn pg-record-store
  "A durable `RecordStore` over Postgres.

  `spec` is a next.jdbc db-spec (`{:dbtype \"postgresql\" :host … :dbname … :user …}`
  or `{:jdbcUrl \"jdbc:postgresql://…\"}`), in which case this builds and owns a
  HikariCP pool — or an existing `javax.sql.DataSource`, which it borrows and does not
  close.  Its own keys, which never reach the driver, are `store-keys`:

  * `:schema` — put the three tables in a schema of their own (created if absent), so
    one database holds several KBs and `DROP SCHEMA` removes one whole;
  * `:cache-capacity` — the per-kind fetch LRU (default 65536);
  * `:premise-cache-capacity` — accepted and ignored.  The strength sets hold every
    premise the `premise-ids` walk read, at a fraction of a byte a handle, and no count
    bounds them;
  * `:fetch-size` — rows per cursor fetch on the enumerations (default 5000);
  * `:pool-size` — maximum pooled connections (default 8);
  * `:prefetch` — act on a caller's prefetch hint (default true).  Whether a hint ever
    arrives is the *caller's* setting (`resolution/*prefetch-candidates*`, off by
    default); this says whether this store does anything when one does, and `false` here
    is the hard off for benchmarking one against the other;
  * `:prefetch-min` — how many handles in a hint must be **uncached** before a batched
    read is issued instead of leaving them to the point reads (default 4).

  Creates the schema if absent and loads the handle counter, so a reopen mints above
  every handle the store has ever held.  The store is `java.io.Closeable`; `close`
  releases the pool it built and drops its durability registration."
  ([spec] (pg-record-store spec {}))
  ([spec opts]
   (let [{:keys [schema cache-capacity fetch-size pool-size prefetch prefetch-min]
          :or   {cache-capacity default-cache-capacity
                 fetch-size     default-fetch-size
                 pool-size      default-pool-size
                 prefetch       true
                 prefetch-min   default-prefetch-min}}
         (merge (when (map? spec) (select-keys spec store-keys)) opts)
         schema (check-schema! schema)
         sql    (statements schema)
         pool?  (not (instance? DataSource spec))
         ds     (if pool?
                  (->pool (apply dissoc spec store-keys) (long pool-size))
                  spec)]
     (try
       (ensure-schema! ds sql)
       (let [store (map->PgRecordStore
                    {:ds         ds
                     :sql        sql
                     :counter    (atom (load-high-water ds sql))
                     :sx-cache   (lru cache-capacity)
                     :j-cache    (lru cache-capacity)
                     :strengths  (HashMap.)
                     :fetch-size (long fetch-size)
                     :cache-cap  (long cache-capacity)
                     :prefetch   (boolean prefetch)
                     :prefetch-min (long prefetch-min)
                     :own-pool?  pool?
                     :dur-id     (atom nil)
                     :lock       (ReentrantReadWriteLock.)})]
         (reset! (:dur-id store)
                 (dur/register!
                  {;; The server owns durability here.  A commit is durable when the
                   ;; server's WAL says so — `synchronous_commit` is the setting, and
                   ;; it is the operator's — and this store holds no client-side
                   ;; buffer there is anything to force.  Registered all the same, for
                   ;; the close: a JVM that exits without a `close!` still releases
                   ;; the pool.
                   :fsync (fn [_] nil)
                   :close (fn [] (when pool? (.close ^HikariDataSource ds)))
                   :label (str "pg-records " (if schema (str schema "@") "") "postgres")}))
         store)
       (catch Throwable t
         (when pool? (.close ^HikariDataSource ds))
         (throw t))))))
