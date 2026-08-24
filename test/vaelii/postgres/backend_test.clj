;; SPDX-License-Identifier: Apache-2.0
;; Copyright © 2026 Vaelii LLC and the Vaelii contributors.
(ns vaelii.postgres.backend-test
  "The `:pg-memory` and `:pg-disk` backends, end to end through `vaelii.core` — the
  wiring the engine reaches by a lazy `requiring-resolve`.  Core cannot exercise this
  itself (the adapter is not on its classpath); here both are, through
  `checkouts/vaelii`, so an open/assert/close/reopen round trip proves the whole path:
  records persist to the database, `close!` releases the pool and the index directory, a
  fresh KB over the same database recovers, and the recovered KB reasons and retracts.

  The two names differ in one axis and it is the interesting one.  `:pg-memory` keeps the
  derived index in RAM and pays a full `reindex` **and** a full `recover` on every start.
  `:pg-disk` puts that index in a durable directory on the machine running the writer, so
  a restart on **that host** skips the rebuild — and a second host, connecting to the same
  database, finds no index and rebuilds from scratch.  The index does not travel with the
  KB, which is what the last test here pins.

  Requiring `vaelii.postgres.record-store` is what puts the adapter on the classpath so
  the lazy resolve in core finds it."
  (:require [clojure.test :refer [deftest is testing]]
            [vaelii.core :as v]
            [vaelii.impl.protocols :as p]
            [vaelii.postgres.record-store :as rec]
            [vaelii.postgres.test-util :as tu])
  (:import [java.io File]))

(defn- with-temp-dir
  "Run `(f dir)` against a fresh empty directory, deleting it afterwards."
  [f]
  (let [dir (File/createTempFile "vaelii-pg-kb" "")]
    (.delete dir)
    (.mkdirs dir)
    (try
      (f (.getAbsolutePath dir))
      (finally
        (doseq [child (reverse (file-seq dir))] (.delete ^File child))))))

(defn- populate!
  "A rule, the fact that fires it, and one unrelated fact."
  [kb]
  (v/assert kb '(implies (p ?x) (q ?x)) 'CxTest)
  (v/assert kb '(p Foo) 'CxTest)
  (v/assert kb '(likes Felix Tuna) 'CxTest)
  kb)

(defn- reasons-and-retracts!
  "The recovered KB answers from what it rebuilt, and its TMS propagates."
  [kb]
  (testing "the durable records recovered"
    (is (seq (v/sentexes-matching kb '(likes Felix Tuna) 'CxTest))
        "a stored fact reads back from the database after a reopen")
    (is (seq (v/sentexes-matching kb '(p Foo) 'CxTest))
        "the premise fact recovered"))
  (testing "the JTMS was rebuilt on open"
    (is (seq (v/sentexes-matching kb '(q Foo) 'CxTest))
        "(q Foo) is re-derived from the recovered rule and premise"))
  (testing "TMS propagation works on the recovered KB"
    (let [pid (:id (first (v/sentexes-matching kb '(p Foo) 'CxTest)))]
      (v/retract! kb pid)
      (is (empty? (v/sentexes-matching kb '(q Foo) 'CxTest))
          "retracting the premise (p Foo) withdraws the derived (q Foo)"))))

;; ---- :pg-memory ---------------------------------------------------------

(deftest a-pg-memory-kb-persists-and-recovers
  (tu/served
   (fn [ds]
     (tu/with-schema ds "vaelii_kb_mem"
       (fn [spec]
         ;; session 1: assert a rule and a fact — forward chaining derives (q Foo) — then
         ;; release the KB (close! must release the pool, or session 2 contends for it).
         (let [kb (v/open-kb {:backend :pg-memory :pg spec})]
           (try (populate! kb)
                (is (seq (v/sentexes-matching kb '(q Foo) 'CxTest))
                    "forward chaining derived (q Foo) in the first session")
                (finally (v/close! kb))))
         ;; session 2: a process restart — a fresh KB over the same database, recovered
         ;; from the durable records alone.
         (let [kb2 (v/open-kb {:backend :pg-memory :pg spec})]
           (try (reasons-and-retracts! kb2)
                (finally (v/close! kb2)))))))))

(deftest a-corpus-loaded-by-copy-opens-as-a-kb
  ;; The bulk story end to end: records land in the database through `COPY` with no KB
  ;; involved — a load on another machine, or an operator's own restore — and a KB then
  ;; opens on them.  It is also where the standing cost of the RAM index is legible.
  ;; With `{:recover? false}` ("open this and read what is stored, do no work") the
  ;; records are all there and the derived index is empty, because a derived index is not
  ;; stored anywhere; the default open rebuilds it, which is O(records) on **every**
  ;; start.  `:pg-disk` is what buys that back, and the tests below are its mirror.
  (tu/served
   (fn [ds]
     (tu/with-schema ds "vaelii_kb_copy"
       (fn [spec]
         ;; real records, built by the engine, harvested from a RAM KB
         (let [src   (v/open-kb {:backend :memory :space (gensym "src")})
               _     (populate! src)
               recs  (:records src)
               sxs   (mapv #(p/get-sentex recs %) (p/sentex-ids recs))
               js    (mapv #(p/get-justification recs %) (p/justification-ids recs))
               prems (p/premise-ids recs)]
           ;; `:premises? false`, then the marks: a derived sentex carries a strength too,
           ;; so the strength column alone would roster conclusions as assumptions.
           (with-open [store (rec/pg-record-store spec)]
             (is (= (count sxs) (rec/copy-sentexes! store sxs {:premises? false})))
             (is (= (count js) (rec/copy-justifications! store js)))
             (doseq [id prems] (p/mark-premise store id (p/premise-strength recs id))))
           (testing "a KB asked to do no work finds the records and no index"
             (let [kb (v/open-kb {:backend :pg-memory :pg spec :recover? false})]
               (try
                 (is (= (count sxs) (count (p/sentex-ids (:records kb))))
                     "every copied record is in the store")
                 (is (zero? (long (p/count-at (:index kb) [])))
                     "and the derived index opened empty, as a derived index does")
                 (finally (v/close! kb)))))
           (testing "and the default open rebuilds the index and the belief from them"
             (let [kb (v/open-kb {:backend :pg-memory :pg spec})]
               (try
                 (is (seq (v/sentexes-matching kb '(likes Felix Tuna) 'CxTest))
                     "a copied fact reads back through the rebuilt index")
                 (is (seq (v/sentexes-matching kb '(q Foo) 'CxTest))
                     "and the copied rule and premise re-derive their conclusion")
                 (finally (v/close! kb)))))))))))

(deftest an-import-lands-through-the-bulk-seam-and-holds-the-same-kb
  ;; `import!` writes its records through `protocols/BulkLoading` when the store has it,
  ;; which over Postgres is `COPY` instead of a round trip per frame.  What that must not
  ;; buy is a different KB: the same dump into a RAM KB and into a server-backed one,
  ;; compared as knowledge.  The engine reaches the fast path without naming this
  ;; namespace — it asks the store for a sink and this store answers a copy stream.
  (tu/served
   (fn [ds]
     (tu/with-schema ds "vaelii_kb_import"
       (fn [spec]
         (with-temp-dir
           (fn [dir]
             (let [dump (str dir "/dump")
                   src  (v/open-kb {:backend :memory :space (gensym "impsrc")})]
               (populate! src)
               (v/export! src dump {:compression :none})
               (let [ram (v/open-kb {:backend :memory :space (gensym "impram")})
                     pg  (v/open-kb {:backend :pg-memory :pg spec})]
                 (try
                   (is (satisfies? p/BulkLoading (:records pg))
                       "the store answers a bulk sink, so the import path takes it")
                   (let [s-ram (v/import! ram dump)
                         s-pg  (v/import! pg dump)]
                     (is (= (dissoc s-ram :elapsed-ms :duration-ms)
                            (dissoc s-pg :elapsed-ms :duration-ms))
                         "the same summary — the same frames read, stored and refused")
                     (is (= :preserved (:handle-policy s-pg))
                         "and the dump's own numbering survived the copy stream"))
                   (is (= (set (v/handles ram)) (set (v/handles pg)))
                       "the same handles at the same numbers")
                   (is (= (into (sorted-map)
                                (for [h (v/handles ram)] [h (:sentence (v/sentex ram h))]))
                          (into (sorted-map)
                                (for [h (v/handles pg)] [h (:sentence (v/sentex pg h))])))
                       "each naming what it named")
                   (is (= (set (p/premise-ids (:records ram)))
                          (set (p/premise-ids (:records pg))))
                       "and the premise marks rode the copy, not a round trip apiece")
                   (is (= (set (v/ask ram '(q ?x) 'CxTest))
                          (set (v/ask pg '(q ?x) 'CxTest)))
                       "so the rule re-derives the same conclusions after the recover")
                   (finally (v/close! ram) (v/close! pg))))))))))))

;; ---- :pg-disk -----------------------------------------------------------

(deftest a-pg-disk-kb-persists-and-recovers
  (tu/served
   (fn [ds]
     (tu/with-schema ds "vaelii_kb_disk"
       (fn [spec]
         (with-temp-dir
           (fn [dir]
             (let [kb (v/open-kb {:backend :pg-disk :pg spec :dir dir})]
               (try (populate! kb)
                    (is (seq (v/sentexes-matching kb '(q Foo) 'CxTest))
                        "forward chaining derived (q Foo) in the first session")
                    (finally (v/close! kb))))
             (testing "the index is a local directory, and the records are not in it"
               (is (.isDirectory (File. (str dir "/index")))
                   "the durable index writes under :dir")
               (is (not (.exists (File. (str dir "/records"))))
                   "and the records are on the server, so nothing writes them here"))
             (let [kb2 (v/open-kb {:backend :pg-disk :pg spec :dir dir})]
               (try (reasons-and-retracts! kb2)
                    (finally (v/close! kb2)))))))))))

(deftest the-pg-disk-index-survives-the-restart-that-empties-a-derived-one
  ;; the whole of what `:pg-disk` buys over `:pg-memory`: the same `{:recover? false}`
  ;; open that finds an empty index there finds a full one here, so the restart pays
  ;; `recover` and not `reindex` on top of it.
  (tu/served
   (fn [ds]
     (tu/with-schema ds "vaelii_kb_disk_idx"
       (fn [spec]
         (with-temp-dir
           (fn [dir]
             (let [kb (v/open-kb {:backend :pg-disk :pg spec :dir dir})]
               (try (populate! kb) (finally (v/close! kb))))
             (let [kb2 (v/open-kb {:backend :pg-disk :pg spec :dir dir :recover? false})]
               (try
                 (is (= (count (p/sentex-ids (:records kb2)))
                        (long (p/count-at (:index kb2) [])))
                     "the durable index opened describing every record on the server")
                 (finally (v/close! kb2)))))))))))

(deftest a-second-host-finds-no-index-and-rebuilds
  ;; The index is derived from the records and this pairing puts the two on different
  ;; machines with different lifetimes, so the index belongs to whichever host last ran
  ;; the writer and does not travel with the KB.  A second host is a second directory
  ;; over the same database: the coverage check sees an index describing none of the
  ;; records and rebuilds it from them, which is correct and is not free.
  (tu/served
   (fn [ds]
     (tu/with-schema ds "vaelii_kb_two_hosts"
       (fn [spec]
         (with-temp-dir
           (fn [host-a]
             (let [kb (v/open-kb {:backend :pg-disk :pg spec :dir host-a})]
               (try (populate! kb) (finally (v/close! kb))))))
         (with-temp-dir
           (fn [host-b]
             (let [kb (v/open-kb {:backend :pg-disk :pg spec :dir host-b})]
               (try
                 (is (seq (v/sentexes-matching kb '(likes Felix Tuna) 'CxTest))
                     "the second host answers, having rebuilt its index from the records")
                 (is (= (count (p/sentex-ids (:records kb)))
                        (long (p/count-at (:index kb) [])))
                     "and the rebuilt index describes all of them")
                 (finally (v/close! kb)))))))))))

;; ---- the database is the KB's, and the schema is what says so -----------

(deftest two-kbs-in-one-database-do-not-see-each-other
  ;; `:schema` rides the `:pg` opt, and it keys the derived index too: a KB whose index
  ;; were shared with another database's would answer every read out of the wrong
  ;; records.
  (tu/served
   (fn [ds]
     (tu/with-schema ds "vaelii_kb_two_a"
       (fn [spec-a]
         (tu/with-schema ds "vaelii_kb_two_b"
           (fn [spec-b]
             (let [a (v/open-kb {:backend :pg-memory :pg spec-a})
                   b (v/open-kb {:backend :pg-memory :pg spec-b})]
               (try
                 (v/assert a '(likes Muffet Tom) 'CxTest)
                 (is (seq (v/sentexes-matching a '(likes Muffet Tom) 'CxTest)))
                 (is (empty? (v/sentexes-matching b '(likes Muffet Tom) 'CxTest))
                     "the second KB's schema holds none of the first's records")
                 (is (zero? (count (p/sentex-ids (:records b))))
                     "and its store is empty, not merely unindexed")
                 (finally (v/close! a) (v/close! b)))))))))))
