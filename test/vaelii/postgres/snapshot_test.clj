;; SPDX-License-Identifier: Apache-2.0
;; Copyright © 2026 Vaelii LLC and the Vaelii contributors.
(ns vaelii.postgres.snapshot-test
  "The Postgres snapshot sink, exercised against a live server, or skipped with a
  printed reason when none answers — `vaelii.postgres.test-util` is the gate and says
  how to point the suite at one."
  (:require [clojure.test :refer [deftest is]]
            [vaelii.core :as v]
            [vaelii.impl.io.snapshot :as snap]
            [vaelii.impl.protocols :as p]
            [vaelii.postgres.snapshot :as pg]
            [vaelii.postgres.test-util :as tu]))

(defn- fresh-image
  "Run `(f image)` against a clean image name, dropping it afterwards."
  [ds name f]
  (pg/ensure-schema! ds)
  (pg/drop-image! ds name)
  (try (f name) (finally (pg/drop-image! ds name))))

;; ---- the sink and source ------------------------------------------------

(deftest a-section-round-trips-through-postgres
  (tu/served
   (fn [ds]
     (fresh-image ds "rt-section"
                  (fn [image]
                    (let [frames [[:a 1] [:b #{2 3}] [:c [4 5 6]] ['(sym) {:m 1}]]]
                      (with-open [sink (pg/pg-sink ds image {:chunk-size 2})]  ; forces >1 chunk
                        (is (= 4 (snap/write-section! sink "s" frames)) "frame count returned")
                        (snap/commit! sink {:format 1 :index-layout 1 :records "r"
                                            :sections {"s" {:count 4}}}))
                      (let [src (pg/pg-source ds image)]
                        (is (= frames (vec (snap/read-section src "s")))
                            "the frames read back identical, across chunk boundaries")
                        (is (= "r" (:records (snap/read-manifest src)))
                            "and the committed manifest is readable"))))))))

(deftest a-section-cross-loads-with-the-memory-medium
  ;; the portability the seam exists to give: a section written to Postgres reads
  ;; back the same frames a memory medium holds for it
  (tu/served
   (fn [ds]
     (fresh-image ds "rt-cross"
                  (fn [image]
                    (let [frames (mapv (fn [i] [(keyword (str "k" i)) #{i (+ 1000 i)}]) (range 25))
                          mem    (snap/memory-medium)]
                      (snap/write-section! mem "x" frames)
                      (with-open [sink (pg/pg-sink ds image {:chunk-size 10})]
                        (snap/write-section! sink "x" frames)
                        (snap/commit! sink {:format 1 :index-layout 1 :records "r"
                                            :sections {"x" {:count (count frames)}}}))
                      (is (= (vec (snap/read-section mem "x"))
                             (vec (snap/read-section (pg/pg-source ds image) "x")))
                          "memory and Postgres return the same section")))))))

;; ---- the index image, through save-index! / load-index! -----------------

(defn- index-entry-set [index]
  (set (map (fn [[k vv]] [k vv]) (p/index-entries index))))

(deftest the-index-image-round-trips-belief-identical
  (tu/served
   (fn [ds]
     (fresh-image ds "rt-index"
                  (fn [image]
                    (let [src-kb (v/open-kb {:backend :memory})
                          _      (do (v/assert src-kb '(likes Muffet Tom) 'CxTest)
                                     (v/assert src-kb '(likes Tom Jerry) 'CxTest)
                                     (v/assert src-kb '(genls Cat Animal) 'CxTest))
                          stamp  "records-fingerprint-A"
                          ;; write the source KB's index projection to Postgres
                          _      (with-open [sink (pg/pg-sink ds image)]
                                   (snap/save-index! sink (:index src-kb) stamp))
                          ;; load it into a fresh, emptied index
                          dst-kb (v/open-kb {:backend :memory})
                          _      (p/clear-index! (:index dst-kb))
                          result (snap/load-index! (pg/pg-source ds image) (:index dst-kb) stamp)]
                      (is (= :replayed (:index result)) "a matching stamp replays the image")
                      (is (pos? (long (:entries result))) "and installs its entries")
                      (is (= (index-entry-set (:index src-kb))
                             (index-entry-set (:index dst-kb)))
                          "the loaded index answers identically to the source's")))))))

(deftest a-mismatched-image-is-discarded-not-trusted
  (tu/served
   (fn [ds]
     (fresh-image ds "rt-mismatch"
                  (fn [image]
                    (let [kb (v/open-kb {:backend :memory})]
                      (v/assert kb '(likes Muffet Tom) 'CxTest)
                      (with-open [sink (pg/pg-sink ds image)]
                        (snap/save-index! sink (:index kb) "stamp-A"))
                      (let [result (snap/load-index! (pg/pg-source ds image) (:index kb) "stamp-B")]
                        (is (= {:index :rebuild :reason :records-differ} result)
                            "an image whose records stamp disagrees is discarded, the caller rebuilds"))))))))

(deftest an-uncommitted-write-leaves-no-image
  ;; the single-transaction rule: a sink closed without commit! rolls the whole
  ;; image back, so read-manifest is nil and the caller rebuilds
  (tu/served
   (fn [ds]
     (fresh-image ds "rt-abort"
                  (fn [image]
                    (let [sink (pg/pg-sink ds image)]
                      (snap/write-section! sink "s" [[:a 1] [:b 2]])
                      (.close ^java.io.Closeable sink))    ; no commit! — rollback
                    (is (nil? (snap/read-manifest (pg/pg-source ds image)))
                        "no committed manifest, so the image reads as absent"))))))
