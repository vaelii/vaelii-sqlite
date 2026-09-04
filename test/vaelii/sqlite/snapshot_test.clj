;; SPDX-License-Identifier: Apache-2.0
;; Copyright © 2026 Vaelii LLC and the Vaelii contributors.
(ns vaelii.sqlite.snapshot-test
  "The SQLite snapshot sink, exercised against a real single-file database.

  Unlike the Postgres sibling there is **no server to gate on**: SQLite is an
  embedded driver over a file, so every test stands up a fresh temp-file database,
  runs, and deletes it.  `lein test` always runs the whole suite."
  (:require [clojure.test :refer [deftest is]]
            [vaelii.core :as v]
            [vaelii.impl.io.snapshot :as snap]
            [vaelii.impl.protocols :as p]
            [vaelii.sqlite.snapshot :as sqlite])
  (:import [java.io File]))

;; ---- fresh single-file database -----------------------------------------

(defn- with-image
  "Run `(f ds image)` against a fresh single-file SQLite database with its schema
  created, deleting the file (and any `-wal` / `-shm` companions) afterward.
  A function, not a macro, so the binding stays plain."
  [image f]
  (let [file (File/createTempFile "vaelii-sqlite-snap" ".db")
        path (.getAbsolutePath file)
        ds   {:dbtype "sqlite" :dbname path}]
    (try
      (sqlite/ensure-schema! ds)
      (f ds image)
      (finally
        (doseq [suffix ["" "-wal" "-shm"]]
          (.delete (File. (str path suffix))))))))

;; ---- the sink and source ------------------------------------------------

(deftest a-section-round-trips-through-sqlite
  (with-image "rt-section"
    (fn [ds image]
      (let [frames [[:a 1] [:b #{2 3}] [:c [4 5 6]] ['(sym) {:m 1}]]]
        (with-open [sink (sqlite/sqlite-sink ds image {:chunk-size 2})]  ; forces >1 chunk
          (is (= 4 (snap/write-section! sink "s" frames)) "frame count returned")
          (snap/commit! sink {:format 1 :index-layout 1 :records "r"
                              :sections {"s" {:count 4}}}))
        (let [src (sqlite/sqlite-source ds image)]
          (is (= frames (vec (snap/read-section src "s")))
              "the frames read back identical, across chunk boundaries")
          (is (= "r" (:records (snap/read-manifest src)))
              "and the committed manifest is readable"))))))

(deftest a-section-cross-loads-with-the-memory-medium
  ;; the portability the protocol exists to give: a section written to SQLite reads
  ;; back the same frames a memory medium holds for it
  (with-image "rt-cross"
    (fn [ds image]
      (let [frames (mapv (fn [i] [(keyword (str "k" i)) #{i (+ 1000 i)}]) (range 25))
            mem    (snap/memory-medium)]
        (snap/write-section! mem "x" frames)
        (with-open [sink (sqlite/sqlite-sink ds image {:chunk-size 10})]
          (snap/write-section! sink "x" frames)
          (snap/commit! sink {:format 1 :index-layout 1 :records "r"
                              :sections {"x" {:count (count frames)}}}))
        (is (= (vec (snap/read-section mem "x"))
               (vec (snap/read-section (sqlite/sqlite-source ds image) "x")))
            "memory and SQLite return the same section")))))

;; ---- the index image, through save-index! / load-index! -----------------

(defn- index-entry-set [index]
  (set (map (fn [[k vv]] [k vv]) (p/index-entries index))))

(deftest the-index-image-round-trips-belief-identical
  (with-image "rt-index"
    (fn [ds image]
      (let [src-kb (v/open-kb {:backend :memory})
            _      (do (v/assert src-kb '(likes Muffet Tom) 'CxTest)
                       (v/assert src-kb '(likes Tom Jerry) 'CxTest)
                       (v/assert src-kb '(genls Cat Animal) 'CxTest))
            stamp  "records-fingerprint-A"
            ;; write the source KB's index projection to SQLite
            _      (with-open [sink (sqlite/sqlite-sink ds image)]
                     (snap/save-index! sink (:index src-kb) stamp))
            ;; load it into a fresh, emptied index
            dst-kb (v/open-kb {:backend :memory})
            _      (p/clear-index! (:index dst-kb))
            result (snap/load-index! (sqlite/sqlite-source ds image) (:index dst-kb) stamp)]
        (is (= :replayed (:index result)) "a matching stamp replays the image")
        (is (pos? (long (:entries result))) "and installs its entries")
        (is (= (index-entry-set (:index src-kb))
               (index-entry-set (:index dst-kb)))
            "the loaded index answers identically to the source's")))))

(deftest a-mismatched-image-is-discarded-not-trusted
  (with-image "rt-mismatch"
    (fn [ds image]
      (let [kb (v/open-kb {:backend :memory})]
        (v/assert kb '(likes Muffet Tom) 'CxTest)
        (with-open [sink (sqlite/sqlite-sink ds image)]
          (snap/save-index! sink (:index kb) "stamp-A"))
        (let [result (snap/load-index! (sqlite/sqlite-source ds image) (:index kb) "stamp-B")]
          (is (= {:index :rebuild :reason :records-differ} result)
              "an image whose records stamp disagrees is discarded, the caller rebuilds"))))))

(deftest an-uncommitted-write-leaves-no-image
  ;; the single-transaction rule: a sink closed without commit! rolls the whole
  ;; image back, so read-manifest is nil and the caller rebuilds
  (with-image "rt-abort"
    (fn [ds image]
      (let [sink (sqlite/sqlite-sink ds image)]
        (snap/write-section! sink "s" [[:a 1] [:b 2]])
        (.close ^java.io.Closeable sink))    ; no commit! — rollback
      (is (nil? (snap/read-manifest (sqlite/sqlite-source ds image)))
          "no committed manifest, so the image reads as absent"))))

;; ---- a database no sink has written --------------------------------------

(deftest a-database-no-sink-has-written-reads-as-absent
  ;; a fresh file with no image tables is the protocol's absent case, not an error:
  ;; `read-manifest` answers nil, `load-index!` reads that as `:absent` and
  ;; rebuilds, and `drop-image!` is the no-op its docstring claims.
  (let [file (File/createTempFile "vaelii-sqlite-snap" ".db")
        path (.getAbsolutePath file)
        ds   {:dbtype "sqlite" :dbname path}]
    (try
      (is (nil? (snap/read-manifest (sqlite/sqlite-source ds "never-written")))
          "no tables reads as no manifest")
      (let [kb (v/open-kb {:backend :memory})]
        (is (= {:index :rebuild :reason :absent}
               (snap/load-index! (sqlite/sqlite-source ds "never-written") (:index kb) "stamp"))
            "so a first-run load-index! rebuilds rather than throwing"))
      (is (do (sqlite/drop-image! ds "never-written") true)
          "and drop-image! is quiet")
      (finally
        (doseq [suffix ["" "-wal" "-shm"]]
          (.delete (File. (str path suffix))))))))
