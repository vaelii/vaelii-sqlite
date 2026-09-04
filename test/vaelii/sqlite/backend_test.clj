;; SPDX-License-Identifier: Apache-2.0
;; Copyright © 2026 Vaelii LLC and the Vaelii contributors.
(ns vaelii.sqlite.backend-test
  "The `:sqlite` records backend, end to end through `vaelii.core` — the wiring the
  engine reaches by a lazy `requiring-resolve`.  Core cannot exercise this itself (the
  adapter is not on its classpath); here both are, through `checkouts/vaelii`, so an
  open/assert/close/reopen round trip proves the whole path: records persist to the file,
  `close!` releases the connection, a fresh KB over the same directory recovers, and the
  recovered KB reasons and retracts.

  Requiring `vaelii.sqlite.record-store` is what puts the adapter on the classpath so the
  lazy resolve in core finds it."
  (:require [clojure.test :refer [deftest is testing]]
            [vaelii.core :as v]
            [vaelii.impl.protocols :as p]
            [vaelii.sqlite.record-store]
            [vaelii.sqlite.snapshot])                 ; keep both lanes loaded together
  (:import [java.io File]))

(defn- with-temp-dir
  "Run `(f dir)` against a fresh empty directory, deleting it (and the SQLite file it
  gets) afterward."
  [f]
  (let [dir (File/createTempFile "vaelii-sqlite-kb" "")]
    (.delete dir)
    (.mkdirs dir)
    (try
      (f (.getAbsolutePath dir))
      (finally
        (doseq [child (reverse (file-seq dir))] (.delete ^File child))))))

(deftest a-sqlite-backed-kb-persists-and-recovers
  (with-temp-dir
    (fn [dir]
      ;; session 1: assert a rule and a fact — forward chaining derives (q Foo) — then
      ;; release the KB (close! must release the JDBC connection or session 2's open
      ;; would contend on the file).
      (let [kb (v/open-kb {:backend :sqlite :dir dir})]
        (try
          (v/assert kb '(implies (p ?x) (q ?x)) 'CxTest)
          (v/assert kb '(p Foo) 'CxTest)
          (v/assert kb '(likes Felix Tuna) 'CxTest)
          (is (seq (v/sentexes-matching kb '(q Foo) 'CxTest))
              "forward chaining derived (q Foo) in the first session")
          (finally (v/close! kb))))
      ;; session 2: a process restart — a fresh KB over the same directory, recovered
      ;; from the durable SQLite records alone.
      (let [kb2 (v/open-kb {:backend :sqlite :dir dir})]
        (try
          (testing "the durable records recovered"
            (is (seq (v/sentexes-matching kb2 '(likes Felix Tuna) 'CxTest))
                "a stored fact reads back from the SQLite file after a reopen")
            (is (seq (v/sentexes-matching kb2 '(p Foo) 'CxTest))
                "the premise fact recovered"))
          (testing "the JTMS was rebuilt on open"
            (is (seq (v/sentexes-matching kb2 '(q Foo) 'CxTest))
                "(q Foo) is re-derived from the recovered rule and premise"))
          (testing "TMS propagation works on the recovered KB"
            (let [pid (:id (first (v/sentexes-matching kb2 '(p Foo) 'CxTest)))]
              (v/retract! kb2 pid)
              (is (empty? (v/sentexes-matching kb2 '(q Foo) 'CxTest))
                  "retracting the premise (p Foo) withdraws the derived (q Foo)")))
          (finally (v/close! kb2)))))))

(deftest the-sqlite-file-lands-in-the-kb-directory
  (with-temp-dir
    (fn [dir]
      (let [kb (v/open-kb {:backend :sqlite :dir dir})]
        (try
          (v/assert kb '(likes Muffet Tom) 'CxTest)
          (finally (v/close! kb)))
        (is (.exists (File. (str dir "/records.sqlite")))
            "the store is a single file named records.sqlite under the KB directory")))))

(deftest an-import-lands-through-the-bulk-protocol-and-holds-the-same-kb
  ;; `import!` writes its records through `protocols/BulkLoading` when the store has one,
  ;; which here is a transaction per batch instead of the transaction-per-record a `put`
  ;; is.  What that must not buy is a different KB: the same dump into a RAM KB and into a
  ;; SQLite-backed one, compared as knowledge and as handles.
  (with-temp-dir
    (fn [dir]
      (let [dump (str dir "/dump")
            src  (v/open-kb {:backend :memory :space (gensym "impsrc")})]
        (v/assert src '(implies (p ?x) (q ?x)) 'CxTest)
        (v/assert src '(p Foo) 'CxTest)
        (v/assert src '(likes Felix Tuna) 'CxTest {:strength :monotonic})
        (v/export! src dump {:compression :none})
        (v/close! src)
        (let [ram (v/open-kb {:backend :memory :space (gensym "impram")})
              sq  (v/open-kb {:backend :sqlite :dir (str dir "/kb")})]
          (try
            (is (satisfies? p/BulkLoading (:records sq))
                "the store answers a bulk sink, so the import path takes it")
            (let [s-ram (v/import! ram dump)
                  s-sq  (v/import! sq dump)]
              (is (= (dissoc s-ram :elapsed-ms :duration-ms)
                     (dissoc s-sq :elapsed-ms :duration-ms))
                  "the same summary — the same frames read, stored and refused")
              (is (= :preserved (:handle-policy s-sq))
                  "and the dump's own numbering survived the batch"))
            (is (= (set (v/handles ram)) (set (v/handles sq)))
                "the same handles at the same numbers")
            (is (= (into (sorted-map)
                         (for [h (v/handles ram)] [h (:sentence (v/sentex ram h))]))
                   (into (sorted-map)
                         (for [h (v/handles sq)] [h (:sentence (v/sentex sq h))])))
                "each naming what it named")
            (is (= (set (p/premise-ids (:records ram)))
                   (set (p/premise-ids (:records sq))))
                "and the premise marks rode the batch")
            (is (= (set (v/ask ram '(q ?x) 'CxTest))
                   (set (v/ask sq '(q ?x) 'CxTest)))
                "so the rule re-derives the same conclusions after the recover")
            (finally (v/close! ram) (v/close! sq))))))))
