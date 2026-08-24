;; SPDX-License-Identifier: Apache-2.0
;; Copyright © 2026 Vaelii LLC and the Vaelii contributors.
(ns vaelii.sqlite.record-store-test
  "The SQLite record store, exercised against a real single-file database.

  There is no server to gate on — SQLite is an embedded driver over a file — so
  every test stands up a fresh temp-file database and deletes it.  The core
  assurance is an **oracle test**: the same op sequence run against a fresh
  in-memory `RecordStore` (the reference backend) and the SQLite store must yield
  identical observations at every step, so the SQLite store is pinned to the
  reference's behaviour rather than to a re-derivation of the contract."
  (:require [clojure.test :refer [deftest is testing]]
            [vaelii.core :as v]
            [vaelii.impl.capabilities :as cap]
            [vaelii.impl.memory :as mem]
            [vaelii.impl.profile :as prof]
            [vaelii.impl.protocols :as p]
            [vaelii.impl.roster :as roster]
            [vaelii.sqlite.record-store :as rec])
  (:import [java.io File]))

;; ---- fresh single-file database -----------------------------------------

(defn- with-temp-db
  "Run `(f ds)` against a fresh temp-file SQLite db-spec, deleting the file (and any
  `-wal` / `-shm` companions) afterward."
  [f]
  (let [file (File/createTempFile "vaelii-sqlite-rec" ".db")
        path (.getAbsolutePath file)]
    (try
      (f {:dbtype "sqlite" :dbname path})
      (finally
        (doseq [suffix ["" "-wal" "-shm"]]
          (.delete (File. (str path suffix))))))))

(defn- fresh-mem
  "An isolated in-memory `RecordStore` — a unique space so no test shares state with
  another through the memory backend's space registry."
  []
  (mem/memory-record-store {:space (gensym "sp")}))

;; a sentex is any map the store persists by :id; real sentexes always carry a
;; :strength field, so the fixtures do too (see the ns docstring on get-sentex).
(defn- sx [sentence strength]
  {:sentence sentence :context 'CxTest :truth :true :strength strength})

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

(deftest sqlite-matches-the-in-memory-reference
  (with-temp-db
    (fn [ds]
      (with-open [store (rec/sqlite-record-store ds)]
        (is (= (exercise (fresh-mem)) (exercise store))
            "every op observes identically to the in-memory RecordStore")))))

;; ---- type preservation: a real sentex round-trips identical -------------

(deftest a-real-sentex-round-trips-type-identical
  (with-temp-db
    (fn [ds]
      (let [kb (v/open-kb {:backend :memory})]
        (v/assert kb '(likes Muffet Tom) 'CxTest)
        (let [id      (first (p/sentex-ids (:records kb)))
              real-sx (p/get-sentex (:records kb) id)]
          (with-open [store (rec/sqlite-record-store ds)]
            (p/put-sentex store real-sx)
            (let [back (p/get-sentex store id)]
              (is (= (class real-sx) (class back))
                  "an AtomicSentex thaws back an AtomicSentex, not a map")
              (is (= real-sx back)
                  "and equal in value, strength and all"))))))))

;; ---- durability: survives a reopen, never reissues a handle -------------

(deftest records-and-premises-survive-a-reopen
  (with-temp-db
    (fn [ds]
      (let [a (with-open [store (rec/sqlite-record-store ds)]
                (let [a (p/put-sentex store (sx '(p A) :default))]
                  (p/put-sentex store (sx '(p B) :monotonic))
                  (p/put-justification store {:informant :fwd})
                  (p/mark-premise store a :monotonic)
                  a))]
        (with-open [store (rec/sqlite-record-store ds)]
          (is (= 2 (count (p/sentex-ids store))) "both sentexes recovered")
          (is (= 1 (count (p/justification-ids store))) "the justification recovered")
          (is (= #{a} (p/premise-ids store)) "the premise mark recovered")
          (is (= :monotonic (p/premise-strength store a))
              "the premise strength recovered from the column")
          (is (= :monotonic (:strength (p/get-sentex store a)))
              "and is reflected on the fetched record"))))))

(deftest a-batch-annotate-lands-what-the-one-row-door-lands
  ;; `mark-premise-batch` / `put-provenance-batch` are one transaction where the
  ;; protocol's own ops are a transaction apiece, so what has to be pinned is that nothing
  ;; about the result moves — the guard against marking a handle with no sentex included,
  ;; which here is the UPDATE's own WHERE and is the same clause either way.
  (with-temp-db
    (fn [ds]
      (with-open [store (rec/sqlite-record-store ds)]
        (let [a    (p/put-sentex store (sx '(p A) nil))
              b    (p/put-sentex store (sx '(p B) nil))
              c    (p/put-sentex store (sx '(p C) nil))
              gone 99999]
          (p/mark-premise-batch store {a :monotonic, b :default, gone :default})
          (testing "the marks land, and a handle with no sentex is not one of them"
            (is (= #{a b} (set (p/premise-ids store))))
            (is (= [:monotonic :default] [(p/premise-strength store a)
                                          (p/premise-strength store b)]))
            (is (nil? (p/get-sentex store gone))))
          (testing "the record reflects the new strength, the cache having been evicted"
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
            (is (nil? (p/get-provenance store c))))
          (testing "an empty batch is a no-op rather than a malformed statement"
            (p/mark-premise-batch store {})
            (p/put-provenance-batch store [])
            (is (= #{a b} (set (p/premise-ids store)))))))))
  (testing "and all of it survives the reopen, which is where a transaction is judged"
    (with-temp-db
      (fn [ds]
        (let [[a b] (with-open [store (rec/sqlite-record-store ds)]
                      (let [a (p/put-sentex store (sx '(p A) nil))
                            b (p/put-sentex store (sx '(p B) nil))]
                        (p/mark-premise-batch store {a :monotonic, b :default})
                        (p/put-provenance-batch store [[a {:by "ann"}]])
                        [a b]))]
          (with-open [store (rec/sqlite-record-store ds)]
            (is (= #{a b} (set (p/premise-ids store))))
            (is (= :monotonic (p/premise-strength store a)))
            (is (= {:by "ann"} (p/get-provenance store a)))))))))

(deftest a-handle-is-never-reissued-across-a-reopen
  ;; the never-reissue guarantee: allocate the max handle, delete it, reopen — the
  ;; counter must not fall back and hand that handle out again.
  (with-temp-db
    (fn [ds]
      (let [top (with-open [store (rec/sqlite-record-store ds)]
                  (p/put-sentex store (sx '(p A) :default))
                  (let [top (p/put-sentex store (sx '(p B) :default))]  ; the max handle
                    (p/delete-sentex! store top)                        ; …now gone
                    top))]
        (with-open [store (rec/sqlite-record-store ds)]
          (let [next (p/put-sentex store (sx '(p C) :default))]
            (is (> next top)
                "the reopened store allocates above the deleted max, never reissuing it")))))))

;; ---- the cache is keyed by the number, not by how it was boxed ----------

(deftest a-delete-evicts-whatever-integer-type-it-arrived-as
  ;; `Integer 5` and `Long 5` are equal numbers and different `java.util.Map` keys, so a
  ;; cache keyed by the raw handle would keep answering with a record the store no longer
  ;; holds — for the life of the store, and with nothing to signal it.  Clojure's own maps
  ;; normalize integer hashing, so the RAM and disk stores cannot show this and only a
  ;; store with a `LinkedHashMap` in front of it can.
  (with-temp-db
    (fn [ds]
      (with-open [store (rec/sqlite-record-store ds)]
        (let [id (p/put-sentex store (sx '(p A) :default))]
          (is (some? (p/get-sentex store id)) "stored, and cached by the put")
          (p/delete-sentex! store (int id))
          (is (empty? (p/sentex-ids store)) "the row is gone")
          (is (nil? (p/get-sentex store id))
              "and so is the cached copy — a deleted record must not keep answering"))))))

(deftest a-fetch-finds-the-cached-record-whatever-integer-type-it-arrives-as
  (with-temp-db
    (fn [ds]
      (with-open [store (rec/sqlite-record-store ds)]
        (let [id (p/put-sentex store (sx '(p A) :default))]
          (is (= (p/get-sentex store id) (p/get-sentex store (int id)))
              "one record, whichever way the handle was boxed"))))))

;; ---- the fetches are tallied -------------------------------------------

(deftest the-three-fetches-are-counted
  ;; `RecordStore` says every implementation tallies its own kind on the protocol method,
  ;; so a caller reading `:fetches` sees what it asked for rather than what a backend did
  ;; about it — and the number is comparable across backends only if each one counts.
  (with-temp-db
    (fn [ds]
      (with-open [store (rec/sqlite-record-store ds)]
        (let [id (p/put-sentex store (sx '(p A) :default))
              jid (p/put-justification store {:informant :fwd})]
          (p/put-provenance store id {:creator :alice})
          (prof/start)
          (p/get-sentex store id)
          (p/get-justification store jid)
          (p/get-provenance store id)
          (let [{:keys [fetches]} (prof/stop)]
            (is (= {:sentex 1 :justification 1 :provenance 1} fetches)
                "one of each kind, counted on the protocol method")))))))

;; ---- clear wipes everything ---------------------------------------------

(deftest clear-records-wipes-and-resets
  (with-temp-db
    (fn [ds]
      (with-open [store (rec/sqlite-record-store ds)]
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
            (is (= 1 (p/next-id store)) "and the counter is reset to mint from 1")))))))

;; ---- the roster, and the questions that do not need it -------------------

(deftest the-enumerations-answer-a-compressed-roster
  ;; The seam says a `java.util.Set`, not a `PersistentHashSet` — and a SQLite store's
  ;; enumeration is a table scan whatever the file is on, so the roster it hands back is
  ;; the caller's heap at 48–75 bytes a handle.  What is asserted is that this store
  ;; answers the compressed one and that it reads as the set the reference backend
  ;; answers; core's `roster_test` owns the shape itself.
  (with-temp-db
    (fn [ds]
      (with-open [store (rec/sqlite-record-store ds)]
        (let [ids (into [] (map #(p/put-sentex store (sx (list 'p %) :default))) (range 200))
              j   (p/put-justification store {:informant :fwd})]
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
            (is (= (sort ids) (sort (p/sentex-ids store))) "and sorts to one sequence")))))))

(deftest a-tally-agrees-with-the-roster
  ;; `open-kb` asks how many records this store holds and whether it holds any, before
  ;; the KB has answered anything.  The property that matters is that the capability and
  ;; the enumeration answer the same thing, since core calls the helpers unconditionally.
  (with-temp-db
    (fn [ds]
      (with-open [store (rec/sqlite-record-store ds)]
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
            (is (contains? (set ids) (cap/some-sentex-id store)))
            (is (contains? (set js) (cap/some-justification-id store)))
            (is (= (nth ids 3) (cap/some-premise-id store))))
          (testing "a delete moves both"
            (p/delete-sentex! store (nth ids 3))
            (is (= 49 (cap/count-sentexes store)))
            (is (nil? (cap/some-premise-id store))
                "the premise row went with the record, so nothing is marked")))))))
