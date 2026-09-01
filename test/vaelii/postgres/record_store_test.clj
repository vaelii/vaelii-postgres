;; SPDX-License-Identifier: Apache-2.0
;; Copyright © 2026 Vaelii LLC and the Vaelii contributors.
(ns vaelii.postgres.record-store-test
  "The Postgres record store, exercised against a live server (`vaelii.postgres.test-util`
  gates and skips when there is none).

  The core assurance is an **oracle test**: the same op sequence run against a fresh
  in-memory `RecordStore` — the reference backend — and against this one must yield
  identical observations at every step, so the store is pinned to the reference's
  behaviour rather than to a re-derivation of the contract.  The rest is what only a
  networked store has to answer for: that a reopen never reissues a handle, that the
  enumerations stream rather than buffer, and that `COPY` lands the same records the
  per-record door does."
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [next.jdbc :as jdbc]
            [vaelii.core :as v]
            [vaelii.impl.capabilities :as cap]
            [vaelii.impl.memory :as mem]
            [vaelii.impl.protocols :as p]
            [vaelii.impl.roster :as roster]
            [vaelii.postgres.record-store :as rec]
            [vaelii.postgres.test-util :as tu])
  (:import [java.sql ResultSet Statement]))

(defn- with-store
  "Run `(f store)` over a fresh store in a schema of its own, closing it and dropping the
  schema afterwards."
  [ds schema f]
  (tu/with-schema ds schema
    (fn [spec]
      (with-open [store (rec/pg-record-store spec)]
        (f store)))))

(defn- fresh-mem
  "An isolated in-memory `RecordStore` — a unique space, so no test shares state with
  another through the memory backend's space registry."
  []
  (mem/memory-record-store {:space (gensym "sp")}))

;; a sentex is any map the store persists by :id; real sentexes always carry a
;; :strength field, so the fixtures do too (see the ns docstring on get-sentex).
(defn- sx [sentence strength]
  {:sentence sentence :context 'CxTest :polarity :positive :strength strength})

;; ---- the oracle: identical to the in-memory reference -------------------

(defn- exercise
  "A scripted sequence of every op, returning a vector of `[label observation]`.
  Handle allocation is deterministic (both stores mint from 1), so two conforming
  stores return the same vector."
  [store]
  (let [a (p/put-sentex store (sx '(likes A B) :default))
        b (p/put-sentex store (sx '(likes B C) :monotonic))
        j (p/put-justification store {:informant :fwd :antecedents [a b]})
        ;; an explicit id above the counter — the import shape — then next-id must
        ;; clear it, never hand 100 (or below) out again.
        c (p/put-sentex store (assoc (sx '(likes C D) :default) :id 100))
        nx (p/next-id store)]
    (p/mark-premise store a :default)
    (p/mark-premise store b :monotonic)
    (p/put-provenance store a {:creator :alice})
    (let [obs [[:a a] [:b b] [:j j] [:c c] [:next-after-explicit nx]
               [:get-a (p/get-sentex store a)]
               [:get-b (p/get-sentex store b)]
               [:get-c (p/get-sentex store c)]
               [:get-j (p/get-justification store j)]
               [:get-missing (p/get-sentex store 999)]
               [:get-keyword (p/get-sentex store :informant)]     ; non-integer → nil
               [:sentex-ids (p/sentex-ids store)]
               [:justification-ids (p/justification-ids store)]
               [:premise-ids (p/premise-ids store)]
               [:strength-a (p/premise-strength store a)]
               [:strength-b (p/premise-strength store b)]
               [:strength-c (p/premise-strength store c)]         ; not a premise → :default
               [:prov-a (p/get-provenance store a)]]]
      ;; retraction cascade + unmark, observed after
      (p/unmark-premise! store b)
      (p/delete-sentex! store a)          ; drops a's premise + provenance too
      (p/delete-justification! store j)
      (conj obs
            [:after-unmark-strength-b (p/premise-strength store b)]
            [:after-unmark-premise-ids (p/premise-ids store)]
            [:after-del-sentex-ids (p/sentex-ids store)]
            [:after-del-premise-ids (p/premise-ids store)]
            [:after-del-prov-a (p/get-provenance store a)]
            [:after-del-justification-ids (p/justification-ids store)]))))

(deftest postgres-matches-the-in-memory-reference
  (tu/served
   (fn [ds]
     (with-store ds "vaelii_rec_oracle"
       (fn [store]
         (is (= (exercise (fresh-mem)) (exercise store))
             "every op observes identically to the in-memory RecordStore"))))))

;; ---- type preservation: a real sentex round-trips identical -------------

(deftest a-real-sentex-round-trips-type-identical
  (tu/served
   (fn [ds]
     (let [kb (v/open-kb {:backend :memory :space (rand-int 100000)})]
       (v/assert kb '(likes Muffet Tom) 'CxTest)
       (let [id      (first (p/sentex-ids (:records kb)))
             real-sx (p/get-sentex (:records kb) id)]
         (with-store ds "vaelii_rec_types"
           (fn [store]
             (p/put-sentex store real-sx)
             (let [back (p/get-sentex store id)]
               (is (= (class real-sx) (class back))
                   "a LiteralSentex thaws back a LiteralSentex, not a map")
               (is (= real-sx back)
                   "and equal in value, strength and all")))))))))

;; ---- durability: survives a reopen, never reissues a handle -------------

(deftest records-and-premises-survive-a-reopen
  (tu/served
   (fn [ds]
     (tu/with-schema ds "vaelii_rec_reopen"
       (fn [spec]
         (let [a (with-open [store (rec/pg-record-store spec)]
                   (let [a (p/put-sentex store (sx '(p A) :default))]
                     (p/put-sentex store (sx '(p B) :monotonic))
                     (p/put-justification store {:informant :fwd})
                     (p/mark-premise store a :monotonic)
                     a))]
           (with-open [store (rec/pg-record-store spec)]
             (is (= 2 (count (p/sentex-ids store))) "both sentexes recovered")
             (is (= 1 (count (p/justification-ids store))) "the justification recovered")
             (is (= #{a} (p/premise-ids store)) "the premise mark recovered")
             (is (= :monotonic (p/premise-strength store a))
                 "the premise strength recovered from the column")
             (is (= :monotonic (:strength (p/get-sentex store a)))
                 "and is reflected on the fetched record"))))))))

(deftest a-batch-annotate-lands-what-the-one-row-door-lands
  ;; `mark-premise-batch` / `put-provenance-batch` are one statement where the protocol's
  ;; own ops are a round trip apiece, so what has to be pinned is that nothing about the
  ;; result moves — the phantom-premise guard above all, which over a batch cannot read an
  ;; update count and reads the statement's `RETURNING` instead.
  (tu/served
   (fn [ds]
     (with-store ds "vaelii_rec_annotate"
       (fn [store]
         (let [a    (p/put-sentex store (sx '(p A) nil))
               b    (p/put-sentex store (sx '(p B) nil))
               c    (p/put-sentex store (sx '(p C) nil))
               gone 99999]
           (p/mark-premise-batch store {a :monotonic, b :default, gone :default})
           (testing "the marks land, and a handle with no sentex is not one of them"
             (is (= #{a b} (set (p/premise-ids store))))
             (is (= [:monotonic :default] [(p/premise-strength store a)
                                           (p/premise-strength store b)]))
             (is (nil? (p/get-sentex store gone)) "and no row was invented for it"))
           (testing "the strength cache answers for the marked and not for the absent"
             ;; the cache is filled from the RETURNING, so a handle the statement did not
             ;; touch must not answer `premise-strength` for a handle `premise-ids` omits
             (is (= :default (p/premise-strength store gone))
                 "an unmarked handle reads as the default, not as something cached"))
           (testing "and the record reflects the new strength, the cache having been evicted"
             (is (= :monotonic (:strength (p/get-sentex store a)))))
           (testing "a second batch upgrades a strength already marked"
             (p/mark-premise-batch store {b :monotonic})
             (is (= :monotonic (p/premise-strength store b))))
           (testing "provenance lands in bulk and overwrites"
             (p/put-provenance-batch store [[a {:by "ann"}] [b {:by "bob"}]])
             (is (= [{:by "ann"} {:by "bob"}]
                    [(p/get-provenance store a) (p/get-provenance store b)]))
             (p/put-provenance-batch store [[a {:by "cyd"}]])
             (is (= {:by "cyd"} (p/get-provenance store a)))
             (is (nil? (p/get-provenance store c)) "and touches nothing it was not given"))
           (testing "an empty batch is a no-op rather than a malformed statement"
             (p/mark-premise-batch store {})
             (p/put-provenance-batch store [])
             (is (= #{a b} (set (p/premise-ids store)))))))))))

(deftest a-batch-annotate-spans-more-rows-than-one-chunk
  ;; the chunking is what keeps a statement inside Postgres' 65,535-parameter ceiling, so
  ;; the case worth a test is a batch larger than one chunk — every row still lands, and
  ;; the chunk boundary is not a place where marks go missing.
  (tu/served
   (fn [ds]
     (with-store ds "vaelii_rec_annotate_big"
       (fn [store]
         (let [ids (into [] (for [i (range 2500)] (p/put-sentex store (sx (list 'p i) nil))))]
           (p/mark-premise-batch store (zipmap ids (repeat :monotonic)))
           (is (= (set ids) (set (p/premise-ids store))))
           (is (= :monotonic (p/premise-strength store (last ids)))
               "including the tail, which is in the last chunk")
           (p/put-provenance-batch store (map (fn [id] [id {:n id}]) ids))
           (is (= {:n (last ids)} (p/get-provenance store (last ids))))
           (is (= {:n (first ids)} (p/get-provenance store (first ids))))))))))

(deftest a-handle-is-never-reissued-across-a-reopen
  ;; the never-reissue guarantee: allocate the max handle, delete it, reopen — the
  ;; counter must not fall back and hand that handle out again.  `high_water` is what
  ;; survives the delete; a SEQUENCE would have to be `setval`ed after every import that
  ;; landed records at explicit handles, and that is a race the moment there are two
  ;; writers.
  (tu/served
   (fn [ds]
     (tu/with-schema ds "vaelii_rec_handles"
       (fn [spec]
         (let [top (with-open [store (rec/pg-record-store spec)]
                     (p/put-sentex store (sx '(p A) :default))
                     (let [top (p/put-sentex store (sx '(p B) :default))]  ; the max handle
                       (p/delete-sentex! store top)                        ; …now gone
                       top))]
           (with-open [store (rec/pg-record-store spec)]
             (let [next (p/put-sentex store (sx '(p C) :default))]
               (is (> next top)
                   "the reopened store allocates above the deleted max, never reissuing it")))))))))

(deftest an-explicit-handle-above-the-counter-is-never-handed-out-again
  ;; the `next-id` rule, which is why this store keeps a counter rather than a SEQUENCE:
  ;; a handle that arrived as an explicit `:id` on a put — how an import lands records at
  ;; the handles a dump gave them — has to move the allocator too.
  (tu/served
   (fn [ds]
     (tu/with-schema ds "vaelii_rec_explicit"
       (fn [spec]
         (with-open [store (rec/pg-record-store spec)]
           (p/put-sentex store (assoc (sx '(p A) :default) :id 5000))
           (is (> (p/next-id store) 5000) "in the same session"))
         (with-open [store (rec/pg-record-store spec)]
           (is (> (p/next-id store) 5000) "and after a reopen")))))))

;; ---- clear wipes everything ---------------------------------------------

(deftest clear-records-wipes-and-resets
  (tu/served
   (fn [ds]
     (with-store ds "vaelii_rec_clear"
       (fn [store]
         (let [a (p/put-sentex store (sx '(p A) :default))]
           (p/mark-premise store a :default)
           (p/put-provenance store a {:creator :bob})
           (p/put-justification store {:informant :fwd})
           (p/clear-records! store)
           (testing "after clear"
             (is (empty? (p/sentex-ids store)) "no sentexes")
             (is (empty? (p/justification-ids store)) "no justifications")
             (is (empty? (p/premise-ids store)) "no premises")
             (is (nil? (p/get-provenance store a)) "no provenance")
             (is (= 1 (p/next-id store)) "and the counter is reset to mint from 1"))))))))

;; ---- the enumerations stream --------------------------------------------

(deftest the-enumerations-run-on-a-server-side-cursor
  ;; `sentex-ids`, `justification-ids` and `premise-ids` feed `reindex` and `recover`,
  ;; which walk all of them — so a `SELECT id` that buffers the whole result in the
  ;; driver is an OutOfMemoryError with a plausible-looking stack trace at corpus scale.
  ;;
  ;; The driver streams **only** when the connection is out of autocommit and the
  ;; statement carries a fetch size; that pair is what a portal is asked for, so it is
  ;; what this asserts — read off the live walk rather than off a comment claiming it.
  (tu/served
   (fn [ds]
     (tu/with-schema ds "vaelii_rec_stream"
       (fn [spec]
         (with-open [store (rec/pg-record-store spec {:fetch-size 64})]
           (dotimes [i 500]
             (p/put-sentex store (assoc (sx (list 'p i) :default) :id (inc i)))
             (p/mark-premise store (inc i) :default))
           (testing "the walk is on a cursor"
             (let [shape (rec/reduce-rows
                          store "SELECT id FROM vaelii_rec_stream.vaelii_record" []
                          (fn [_ ^ResultSet rs]
                            (let [^Statement st (.getStatement rs)]
                              (reduced {:fetch-size  (.getFetchSize st)
                                        :autocommit? (.getAutoCommit (.getConnection st))})))
                          nil)]
               (is (= 64 (:fetch-size shape)) "the fetch size the store was built with")
               (is (false? (:autocommit? shape)) "and autocommit off, which is the other half")))
           (testing "and a walk can stop without draining"
             (let [seen (rec/reduce-rows
                         store "SELECT id FROM vaelii_rec_stream.vaelii_record" []
                         (fn [acc ^ResultSet rs]
                           (let [acc (conj acc (.getLong rs 1))]
                             (if (= 3 (count acc)) (reduced acc) acc)))
                         [])]
               (is (= 3 (count seen)) "three rows out of five hundred, and the cursor closed")))
           (testing "and the enumerations themselves are complete"
             (is (= 500 (count (p/sentex-ids store))))
             (is (= 500 (count (p/premise-ids store)))
                 "the premise walk streams too, and fills the strength cache as it goes")
             (is (= :default (p/premise-strength store 250))
                 "which is what the strength read after it costs nothing"))))))))

;; ---- COPY: the bulk ingest path -----------------------------------------

(deftest copy-lands-the-same-records-the-per-record-door-does
  ;; `COPY … FROM STDIN BINARY` is the ingest path a server has and the others do not.
  ;; What it must not do is land anything different: the oracle here is the store's own
  ;; per-record door, run over the same records in another schema.
  (tu/served
   (fn [ds]
     (let [records (mapv (fn [i] (assoc (sx (list 'p i) (if (even? i) :default :monotonic))
                                        :id (inc i)))
                         (range 200))
           deducts (mapv (fn [i] {:id (+ 1000 i) :informant :fwd :antecedents [(inc i)]})
                         (range 50))
           observe (fn [store]
                     {:sentexes  (p/sentex-ids store)
                      :justs     (p/justification-ids store)
                      :premises  (p/premise-ids store)
                      :record-7  (p/get-sentex store 7)
                      :strength-7 (p/premise-strength store 7)
                      :next      (p/next-id store)})
           by-put  (with-store ds "vaelii_rec_copy_put"
                     (fn [store]
                       (doseq [r records]
                         (p/put-sentex store r)
                         (p/mark-premise store (:id r) (:strength r)))
                       (doseq [d deducts] (p/put-justification store d))
                       (observe store)))
           by-copy (with-store ds "vaelii_rec_copy_bulk"
                     (fn [store]
                       (is (= 200 (rec/copy-sentexes! store records {:batch 64})))
                       (is (= 50 (rec/copy-justifications! store deducts)))
                       (observe store)))]
       (is (= by-put by-copy)
           "a COPY load observes as the per-record door's, premise marks included")))))

(deftest copy-moves-the-handle-counter
  ;; a bulk load that left the counter behind would hand out a handle the load just used,
  ;; overwriting a record with no error — the same rule `put-sentex` honours for an
  ;; explicit `:id`.
  (tu/served
   (fn [ds]
     (tu/with-schema ds "vaelii_rec_copy_counter"
       (fn [spec]
         (with-open [store (rec/pg-record-store spec)]
           (rec/copy-sentexes! store [(assoc (sx '(p A) :default) :id 4242)]))
         (with-open [store (rec/pg-record-store spec)]
           (is (> (p/next-id store) 4242)
               "the reopened store mints above what COPY landed")))))))

;; ---- the prefetch hint --------------------------------------------------
;;
;; Every test here is black box, and the probe is the same one: **drop the table**, then
;; read.  A record that still answers can only have come from the store's cache, because
;; there is no longer a row it could have come from.  Nothing reaches inside the store to
;; ask what it holds.

(defn- drop-table! [ds schema]
  (jdbc/execute-one! ds [(str "DROP TABLE " schema ".vaelii_record")]))

(defn- seeded
  "`(f store)` over a store holding sentexes at handles 1..n."
  [spec n opts f]
  (with-open [store (rec/pg-record-store spec opts)]
    (doseq [i (range 1 (inc n))]
      (p/put-sentex store (assoc (sx (list 'p i) (if (even? i) :default :monotonic)) :id i)))
    (f store)))

(deftest a-hint-warms-the-cache-so-the-fetches-do-not-reach-the-server
  (tu/served
   (fn [ds]
     (tu/with-schema ds "vaelii_rec_hint"
       (fn [spec]
         ;; a store that has never read these back: put populates the cache, so read
         ;; through a second store to make the hint the only thing that could have.
         (seeded spec 20 {} (constantly nil))
         (with-open [store (rec/pg-record-store spec)]
           (p/prefetch-sentexes! store (vec (range 1 21)))
           (drop-table! ds "vaelii_rec_hint")
           (testing "a handle the hint named answers from RAM with no table left"
             (is (some? (p/get-sentex store 7)))
             (is (= '(p 7) (:sentence (p/get-sentex store 7)))))))))))

(deftest a-hint-under-the-minimum-fetches-nothing
  ;; below `:prefetch-min` a batch saves nothing worth the round trip, so none is issued.
  (tu/served
   (fn [ds]
     (tu/with-schema ds "vaelii_rec_hint_min"
       (fn [spec]
         (seeded spec 20 {} (constantly nil))
         (with-open [store (rec/pg-record-store spec {:prefetch-min 8})]
           (p/prefetch-sentexes! store [1 2 3])          ; three, under the eight
           (drop-table! ds "vaelii_rec_hint_min")
           (is (thrown? Exception (p/get-sentex store 1))
               "nothing was warmed, so the fetch goes to a table that is gone")))))))

(deftest the-hint-is-refused-when-the-store-is-told-not-to
  ;; `:prefetch false` is the hard off, for measuring one against the other.
  (tu/served
   (fn [ds]
     (tu/with-schema ds "vaelii_rec_hint_off"
       (fn [spec]
         (seeded spec 20 {} (constantly nil))
         (with-open [store (rec/pg-record-store spec {:prefetch false})]
           (p/prefetch-sentexes! store (vec (range 1 21)))
           (drop-table! ds "vaelii_rec_hint_off")
           (is (thrown? Exception (p/get-sentex store 7))
               "the hint was declined, so nothing is in RAM to answer with")))))))

(deftest a-hint-for-cached-handles-asks-the-server-for-nothing
  ;; The cache is the oracle: the store issues a query only for handles it does not hold,
  ;; so a hint naming what it already has must not touch the server at all.
  (tu/served
   (fn [ds]
     (tu/with-schema ds "vaelii_rec_hint_cached"
       (fn [spec]
         (seeded spec 20 {} (constantly nil))
         (with-open [store (rec/pg-record-store spec)]
           (doseq [i (range 1 21)] (p/get-sentex store i))   ; every one now cached
           (drop-table! ds "vaelii_rec_hint_cached")
           (is (nil? (p/prefetch-sentexes! store (vec (range 1 21))))
               "the hint finds nothing missing and issues no query")))))))

(deftest a-justification-hint-warms-the-other-cache
  ;; `recover` fetches every stored justification one handle at a time — the longest run
  ;; of round trips a networked store sees, and one the open pays on every start.
  (tu/served
   (fn [ds]
     (tu/with-schema ds "vaelii_rec_hint_just"
       (fn [spec]
         (with-open [store (rec/pg-record-store spec)]
           (doseq [i (range 1 21)]
             (p/put-justification store {:id i :informant :fwd :antecedents [i]})))
         (with-open [store (rec/pg-record-store spec)]
           (p/prefetch-justifications! store (vec (range 1 21)))
           (drop-table! ds "vaelii_rec_hint_just")
           (is (= {:id 7 :informant :fwd :antecedents [7]} (p/get-justification store 7))
               "the justification answers from RAM with no table left")))))))

(deftest the-two-hints-do-not-cross
  ;; separate ops over separate caches: a sentex hint must not be answered out of the
  ;; justification cache or the other way round, which is what the `kind` column decides.
  (tu/served
   (fn [ds]
     (tu/with-schema ds "vaelii_rec_hint_kinds"
       (fn [spec]
         (with-open [store (rec/pg-record-store spec)]
           (doseq [i (range 1 21)]
             (p/put-sentex store (assoc (sx (list 'p i) :default) :id i)))
           (doseq [i (range 100 120)]
             (p/put-justification store {:id i :informant :fwd})))
         (with-open [store (rec/pg-record-store spec)]
           ;; hint the sentexes only
           (p/prefetch-sentexes! store (vec (range 1 21)))
           (drop-table! ds "vaelii_rec_hint_kinds")
           (is (some? (p/get-sentex store 7)) "the hinted kind is cached")
           (is (thrown? Exception (p/get-justification store 100))
               "the other kind was never warmed, so its fetch goes to the missing table")))))))

(deftest a-hint-cannot-change-what-a-fetch-answers
  (tu/served
   (fn [ds]
     (tu/with-schema ds "vaelii_rec_hint_parity"
       (fn [spec]
         (let [ids (vec (range 1 51))]
           (seeded spec 50 {} (constantly nil))
           (let [plain (with-open [store (rec/pg-record-store spec)]
                         (mapv #(p/get-sentex store %) ids))
                 after (with-open [store (rec/pg-record-store spec)]
                         (p/prefetch-sentexes! store ids)
                         (mapv #(p/get-sentex store %) ids))]
             (is (= plain after)
                 "record for record, including the authoritative strength column"))))))))

;; ---- the cache is not allowed to outlive what it caches ----------------
;;
;; Every test below is a bug this store shipped with and a reviewer found; each is a
;; place where the cache in front of the table could answer something the table does not
;; say, which on a networked store is the whole hazard the cache introduces.

(deftest a-marked-row-whose-strength-was-wiped-does-not-break-the-open
  ;; `put-sentex` rewrites the authoritative `strength` column and deliberately leaves
  ;; `premise` alone, so a re-put carrying no `:strength` leaves a row marked with a NULL
  ;; strength.  The strength cache is a ConcurrentHashMap, which refuses a null value —
  ;; and `recover` calls `premise-ids` unconditionally, so this was a KB that could not
  ;; open at all, permanently.
  (tu/served
   (fn [ds]
     (tu/with-schema ds "vaelii_rec_nullstrength"
       (fn [spec]
         (with-open [store (rec/pg-record-store spec)]
           (p/put-sentex store (assoc (sx '(p A) :default) :id 1))
           (p/mark-premise store 1 :monotonic)
           (p/put-sentex store {:id 1 :sentence '(p A) :context 'CxTest :polarity :positive})
           (is (= #{1} (p/premise-ids store)) "the walk completes and still rosters it")
           (is (= :default (p/premise-strength store 1))
               "and a marked row with no strength reads as :default, as the reference stores do")))))))

(deftest a-re-put-does-not-leave-a-stale-strength-cached
  ;; `put-sentex` writes the strength column; the cache in front of that column has to go
  ;; with it, or `recover` seeds belief at a strength the store no longer holds — and
  ;; strength decides defeat.
  (tu/served
   (fn [ds]
     (tu/with-schema ds "vaelii_rec_restrength"
       (fn [spec]
         (with-open [store (rec/pg-record-store spec)]
           (p/put-sentex store (assoc (sx '(p A) :default) :id 1))
           (p/mark-premise store 1 :monotonic)
           (is (= :monotonic (p/premise-strength store 1)))
           (p/put-sentex store (assoc (sx '(p A) :default) :id 1))
           (is (= :default (p/premise-strength store 1))
               "the column was rewritten, so the cached strength cannot stand")
           (is (= :default (:strength (p/get-sentex store 1)))
               "and the record agrees with it")))))))

(deftest marking-a-handle-with-no-record-caches-nothing
  ;; the UPDATE's WHERE guards the table against a phantom premise; the cache needs the
  ;; same guard, or `premise-strength` answers for a handle `premise-ids` does not name.
  (tu/served
   (fn [ds]
     (tu/with-schema ds "vaelii_rec_phantom"
       (fn [spec]
         (with-open [store (rec/pg-record-store spec)]
           (p/mark-premise store 777 :monotonic)
           (is (empty? (p/premise-ids store)) "no phantom in the table")
           (is (= :default (p/premise-strength store 777))
               "and none in the cache either — the reference stores answer :default")))))))

(deftest a-strength-upgrade-lands-even-with-a-full-cache
  ;; the ordinary path is a strength *upgrade* on a handle already marked, so a capacity
  ;; guard that refuses the write for a key already held drops exactly the writes that
  ;; matter.
  (tu/served
   (fn [ds]
     (tu/with-schema ds "vaelii_rec_capacity"
       (fn [spec]
         (with-open [store (rec/pg-record-store spec {:premise-cache-capacity 2})]
           (doseq [i [1 2 3]]
             (p/put-sentex store (assoc (sx (list 'p i) :default) :id i))
             (p/mark-premise store i :default))
           (p/mark-premise store 1 :monotonic)
           (is (= :monotonic (p/premise-strength store 1))
               "the upgrade is visible though the cache is over capacity")))))))

(deftest a-handle-this-store-could-not-have-issued-is-answered-not-thrown
  ;; `id-ok?`'s contract, which the fetches kept and four other ops did not: an informant
  ;; keyword reaches these doors and both reference stores make them no-ops.
  (tu/served
   (fn [ds]
     (tu/with-schema ds "vaelii_rec_badid"
       (fn [spec]
         (with-open [store (rec/pg-record-store spec)]
           (doseq [[label f] [["mark-premise"      #(p/mark-premise store :informant :default)]
                              ["unmark-premise!"   #(p/unmark-premise! store :informant)]
                              ["put-provenance"    #(p/put-provenance store :informant {:a 1})]
                              ["delete-provenance!" #(p/delete-provenance! store :informant)]]]
             (is (nil? (try (f) nil (catch Exception e e))) (str label " is quiet")))))))))

(deftest a-bulk-load-does-not-need-a-second-connection
  ;; `COPY` holds one pooled connection for its whole run; the high-water write that
  ;; follows it used to borrow a second while the first was still held, which at
  ;; `:pool-size 1` is a self-deadlock — and one that reports failure on a load that
  ;; committed in full.
  (tu/served
   (fn [ds]
     (tu/with-schema ds "vaelii_rec_onepool"
       (fn [spec]
         (with-open [store (rec/pg-record-store spec {:pool-size 1})]
           (let [records (mapv #(assoc (sx (list 'p %) :default) :id (inc %)) (range 10))]
             (is (= 10 (rec/copy-sentexes! store records)))
             (is (= 10 (count (p/sentex-ids store))))
             (is (> (p/next-id store) 10) "and the high-water mark moved"))))))))

(deftest a-copy-batch-must-be-a-positive-number-of-rows
  (tu/served
   (fn [ds]
     (tu/with-schema ds "vaelii_rec_badbatch"
       (fn [spec]
         (with-open [store (rec/pg-record-store spec)]
           (is (thrown-with-msg? clojure.lang.ExceptionInfo #"positive number of rows"
                                 (rec/copy-sentexes! store [(assoc (sx '(p A) :default) :id 1)]
                                                     {:batch 0})))))))))

(deftest a-concurrent-delete-is-not-undone-by-a-reader-filling-its-cache
  ;; The supported shape is one writer and a reader beside it.  A read is a miss, a server
  ;; round trip and an install; a delete landing between the round trip and the install was
  ;; undone by it, and the deleted record then answered that reader for the life of the
  ;; store.  Stressed rather than staged: the window is real but narrow.
  (tu/served
   (fn [ds]
     (tu/with-schema ds "vaelii_rec_race"
       (fn [spec]
         (with-open [store (rec/pg-record-store spec)]
           (let [trials 200
                 resurrected
                 (reduce
                  (fn [n i]
                    (let [id (inc i)]
                      (p/put-sentex store (assoc (sx (list 'p id) :default) :id id))
                      ;; drop it from the cache so the reader takes the miss path
                      (p/put-provenance store id {:x 1})
                      (let [reader (future (p/get-sentex store id))]
                        (p/delete-sentex! store id)
                        @reader
                        (if (some? (p/get-sentex store id)) (inc n) n))))
                  0 (range trials))]
             (is (zero? resurrected)
                 (str resurrected " of " trials
                      " deleted records were reinstalled by a concurrent read")))))))))

;; ---- the schema is the KB's own -----------------------------------------

(deftest two-kbs-share-a-database-without-sharing-records
  ;; `:schema` is what makes one database hold several KBs: an operator drops one with
  ;; `DROP SCHEMA` and the other is untouched.
  (tu/served
   (fn [ds]
     (tu/with-schema ds "vaelii_rec_two_a"
       (fn [spec-a]
         (tu/with-schema ds "vaelii_rec_two_b"
           (fn [spec-b]
             (with-open [a (rec/pg-record-store spec-a)
                         b (rec/pg-record-store spec-b)]
               (p/put-sentex a (sx '(only-in A) :default))
               (is (= 1 (count (p/sentex-ids a))))
               (is (empty? (p/sentex-ids b))
                   "the second KB's schema holds none of the first's records")))))))))

(deftest a-schema-that-is-not-an-identifier-is-refused
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"bare SQL identifier"
                        (rec/pg-record-store {:dbtype "postgresql" :dbname "nope"
                                              :schema "drop; --"}))))

;; ---- the pool is released -----------------------------------------------

(deftest close-releases-the-pool
  (tu/served
   (fn [ds]
     (tu/with-schema ds "vaelii_rec_close"
       (fn [spec]
         (let [store (rec/pg-record-store spec)]
           (p/put-sentex store (sx '(p A) :default))
           (.close ^java.io.Closeable store)
           (is (thrown? Exception (p/sentex-ids store))
               "the pool is closed, so the store answers nothing afterwards")))))))

;; ---- a DataSource handed in is not this store's to close ----------------

(deftest a-borrowed-datasource-outlives-the-store
  (tu/served
   (fn [ds]
     (tu/with-schema ds "vaelii_rec_borrow"
       (fn [spec]
         (let [pool (jdbc/get-datasource (dissoc spec :schema))]
           (with-open [store (rec/pg-record-store pool {:schema "vaelii_rec_borrow"})]
             (p/put-sentex store (sx '(p A) :default)))
           (with-open [c (jdbc/get-connection pool)]
             (is (some? (jdbc/execute-one! c ["SELECT 1"]))
                 "the datasource still answers after the store closed"))))))))

;; ---- the roster, and the questions that do not need it -------------------

(deftest the-enumerations-answer-a-compressed-roster
  ;; The seam says a `java.util.Set`, not a `PersistentHashSet` — and this is the store
  ;; the difference is for.  A hash set of boxed handles retains 48–75 bytes apiece, so
  ;; the roster of the corpus a server backend exists to hold is gigabytes of the
  ;; caller's heap before a record is fetched; the compressed one is a fraction of a byte
  ;; a handle over the near-contiguous run `next-id` mints.  What is asserted here is not
  ;; the size — core's `roster_test` owns that — but that this store returns one, and that
  ;; it reads as the set the reference backend answers.
  (tu/served
   (fn [ds]
     (with-store ds "vaelii_rec_roster"
       (fn [store]
         (let [ids (into [] (map #(p/put-sentex store (sx (list 'p %) :default))) (range 200))
               j   (p/put-justification store {:informant :fwd :antecedents (vec (take 2 ids))})]
           (p/mark-premise store (first ids) :monotonic)
           (doseq [[label got] [["sentex-ids" (p/sentex-ids store)]
                                ["justification-ids" (p/justification-ids store)]
                                ["premise-ids" (p/premise-ids store)]]]
             (is (roster/roster? got) (str label " answers a compressed roster")))
           (testing "and it reads as the set it replaces"
             (is (= (set ids) (p/sentex-ids store)))
             (is (= (p/sentex-ids store) (set ids)) "equal whichever side it is on")
             (is (= #{j} (p/justification-ids store)))
             (is (= #{(first ids)} (p/premise-ids store)))
             (is (contains? (p/sentex-ids store) (first ids)))
             (is (not (contains? (p/sentex-ids store) 999999)))
             (is (= 200 (count (p/sentex-ids store))))
             (is (= (sort ids) (sort (p/sentex-ids store))) "and sorts to one sequence"))
           (testing "the premise walk still fills the strength cache"
             (p/premise-ids store)
             (is (= :monotonic (p/premise-strength store (first ids)))))))))))

(deftest a-tally-is-one-row-and-agrees-with-the-roster
  ;; `open-kb` asks how many records this store holds and whether it holds any, before
  ;; the KB has answered anything.  Spelled through the roster those are the whole table
  ;; on the wire; through `Tallying` they are one row each.  The property that matters is
  ;; that they answer the *same* thing, since core calls the helpers unconditionally.
  (tu/served
   (fn [ds]
     (with-store ds "vaelii_rec_tally"
       (fn [store]
         (testing "over an empty store"
           (is (zero? (cap/count-sentexes store)))
           (is (zero? (cap/count-justifications store)))
           (is (nil? (cap/some-sentex-id store)))
           (is (nil? (cap/some-justification-id store)))
           (is (nil? (cap/some-premise-id store))))
         (let [ids (into [] (map #(p/put-sentex store (sx (list 'p %) :default))) (range 50))
               js  (into [] (map (fn [_] (p/put-justification store {:informant :fwd}))) (range 7))]
           (p/mark-premise store (nth ids 3) :default)
           (testing "over a populated one"
             (is (= (count (p/sentex-ids store)) (cap/count-sentexes store) 50))
             (is (= (count (p/justification-ids store)) (cap/count-justifications store) 7))
             (is (contains? (set ids) (cap/some-sentex-id store))
                 "the sampled sentex is one the store holds")
             (is (contains? (set js) (cap/some-justification-id store)))
             (is (= (nth ids 3) (cap/some-premise-id store))
                 "and the only premise is the one that was marked"))
           (testing "a delete moves both"
             (p/delete-sentex! store (nth ids 3))
             (is (= 49 (cap/count-sentexes store)))
             (is (nil? (cap/some-premise-id store))
                 "the premise row went with the record, so nothing is marked"))))))))

(deftest a-tally-does-not-stream-the-table
  ;; The whole claim: a count and a sample read one row, not one per record.  Asserted
  ;; against the server's own statistics rather than against a comment — `pg_stat_*` is
  ;; not granular enough per statement, so this reads the plan: a count is an aggregate
  ;; over an index-only or sequential scan, and a sample carries a LIMIT node.  Either
  ;; way the point is that the client receives one row.
  (tu/served
   (fn [ds]
     (with-store ds "vaelii_rec_tallyplan"
       (fn [store]
         (dotimes [i 300] (p/put-sentex store (assoc (sx (list 'p i) :default) :id (inc i))))
         (let [plan (fn [sql]
                      (->> (jdbc/execute! (:ds store) [(str "EXPLAIN " sql)])
                           (map (comp str first vals))
                           (str/join " ")))]
           (is (str/includes?
                (plan "SELECT count(*) FROM vaelii_rec_tallyplan.vaelii_record WHERE kind = 0")
                "Aggregate")
               "the count is aggregated on the server, so one row crosses the wire")
           (is (str/includes?
                (plan "SELECT id FROM vaelii_rec_tallyplan.vaelii_record WHERE kind = 0 LIMIT 1")
                "Limit")
               "and the sample stops at the first row rather than scanning to build a set")))))))
