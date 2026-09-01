;; SPDX-License-Identifier: Apache-2.0
;; Copyright © 2026 Vaelii LLC and the Vaelii contributors.
(ns vaelii.sqlite.record-store
  "A SQLite target for the engine's **record store** seam
  (`vaelii.impl.protocols/RecordStore`) — the durable ground truth a KB is
  recovered from: canonical sentexes and justifications, keyed by integer handle,
  in a single SQLite file.

  This is the **other SQLite lane**, and the one the snapshot sink argues around.
  A records store over a *networked* database pays a round trip per probe — two
  orders of magnitude off local, which is why that lane stays a snapshot.
  **Embedded SQLite is not networked**: the driver runs in-process against a file,
  a warm point read is single-digit microseconds (measured ~6µs against the disk
  store's ~3µs), so a live records backend is a real third option beside `:memory`
  and `:disk` — an always-current, single-file, SQL-inspectable store, where the
  disk backend is a bespoke mmap pair and the snapshot is a frozen image you
  re-export.

  ## The shape

  The seam is 17 ops (`put-`/`get-`/`delete-` for sentexes, justifications and
  provenance; `next-id`; the `*-ids` enumerations; premise marking).  Here:

  * every record is one row in `record (id, kind, frame, premise, strength)` — the
    handle is the primary key, `kind` splits sentexes (0) from justifications (1),
    and `frame` is the **whole record** nippy-frozen, so a fetch thaws back
    type-identical (`LiteralSentex` stays a `LiteralSentex`);
  * a sentex's **assumption strength** rides the `strength` column as the
    authoritative value and is `assoc`ed back onto the thawed record on read — so
    `mark-premise` is a one-row column update, never a frame rewrite, and the
    column and frame cannot drift because the column always wins;
  * **handles are never reissued.**  `next-id` walks an in-memory counter, and a
    monotonic `high_water` row (bumped inside each put's transaction, reconciled
    with the live max on open) survives a delete-of-the-max-handle across a
    reopen — the disk store gets the same guarantee from an idx file that never
    shrinks.

  ## The file

  Point the store at a **file-backed** db-spec (`{:dbtype \"sqlite\" :dbname
  \"kb.db\"}`).  It holds **one long-lived connection** for its lifetime under
  WAL + `synchronous=NORMAL` — the disk store's durability shape (buffered writes,
  periodic fsync, a crash loses at most the last unsynced writes, never
  consistency).  A JDBC connection is not thread-safe, so every op serializes on a
  lock, with a synchronized access-ordered LRU in front so a hot read never
  reaches SQLite or the lock.  `close`/`clear-records!` release it.  It is
  `java.io.Closeable`.

  ## Boundary

  Apache-2.0, and an **adapter**: it implements the SSPL engine's
  `vaelii.impl.protocols/RecordStore` and is never depended on by it.  Core wires
  it in by a lazy `requiring-resolve`, the same way it reaches the dense TMS, so
  the engine never loads the SQLite driver unless a KB asks for this backend."
  (:require [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [taoensso.nippy :as nippy]
            [vaelii.impl.disk.durability :as dur]
            [vaelii.impl.profile :as prof]
            [vaelii.impl.protocols :as p]
            [vaelii.impl.roster :as roster])
  (:import [java.sql Connection]
           [java.util ArrayList Collections LinkedHashMap Map]
           [java.util.concurrent.locks ReentrantLock]))

;; ---- schema -------------------------------------------------------------

(def ^:private kind-sentex 0)
(def ^:private kind-justification 1)

(def ^:private record-ddl
  "CREATE TABLE IF NOT EXISTS record (
     id       integer PRIMARY KEY,
     kind     integer NOT NULL,
     frame    blob    NOT NULL,
     premise  integer NOT NULL DEFAULT 0,
     strength text)")

;; premise-ids is `WHERE kind=0 AND premise=1`; a partial index keeps it O(premises)
;; rather than a full-table scan on a store that is mostly non-premise derivations.
(def ^:private premise-index-ddl
  "CREATE INDEX IF NOT EXISTS record_premise ON record (id) WHERE premise = 1")

(def ^:private provenance-ddl
  "CREATE TABLE IF NOT EXISTS record_provenance (
     id   integer PRIMARY KEY,
     prov blob    NOT NULL)")

;; a one-row-per-key scalar table; the only key today is 'high_water', the largest
;; handle ever allocated — the never-reissue guarantee across a reopen.
(def ^:private meta-ddl
  "CREATE TABLE IF NOT EXISTS record_meta (
     k text    PRIMARY KEY,
     v integer NOT NULL)")

(defn- id-roster
  "Every handle of `kind` (optionally narrowed by `and-clause`) as a
  `vaelii.impl.roster` set.

  A roster and not a `PersistentHashSet<Long>` for the reason core's `docs/storage.md`
  gives: the hash set retains 48–75 bytes a handle, and handles arrive in the
  near-contiguous run `next-id` mints, which a bitmap holds at a fraction of a byte
  apiece and probes faster.  `plan` rather than `execute!` so the walk never builds a
  result map per row on the way to discarding it — an embedded driver pays no round trip,
  but it pays that allocation."
  [conn kind and-clause]
  (let [[add! finish] (roster/collector)]
    (reduce (fn [acc row] (add! (:id row)) acc)
            nil
            (jdbc/plan conn [(str "SELECT id FROM record WHERE kind=?" and-clause) kind]))
    (finish)))

(defn ensure-schema!
  "Create the record, provenance and meta tables (and the premise index) if absent.
  Idempotent.  Runs on `conn` in autocommit before any op."
  [^Connection conn]
  (jdbc/execute-one! conn [record-ddl])
  (jdbc/execute-one! conn [premise-index-ddl])
  (jdbc/execute-one! conn [provenance-ddl])
  (jdbc/execute-one! conn [meta-ddl]))

;; ---- helpers ------------------------------------------------------------

(defmacro ^:private locked
  "Run `body` holding the ReentrantLock `lk` — one JDBC connection, one writer."
  [lk & body]
  `(let [^ReentrantLock l# ~lk]
     (.lock l#)
     (try ~@body (finally (.unlock l#)))))

(defn- lru
  "A synchronized, access-ordered LRU bounded at `cap` entries — the disk store's
  per-kind fetch cache, so a hot read never reaches SQLite."
  ^Map [^long cap]
  (Collections/synchronizedMap
   (proxy [LinkedHashMap] [16 (float 0.75) true]
     (removeEldestEntry [_] (> (.size ^LinkedHashMap this) cap)))))

(defn- frame-of ^bytes [row] (:frame row))

(def ^:private lower-maps {:builder-fn rs/as-unqualified-lower-maps})

(defn- id-ok?
  "The record protocol is asked for non-integer / negative handles — an informant
  keyword lands here — and must answer nil, not throw.  Gate every fetch on this."
  [id]
  (and (integer? id) (not (neg? ^long id))))

;; Every cache is keyed by the **boxed long**, never by whatever integer type the caller
;; happened to hold.  `Integer 5` and `Long 5` are equal numbers and different
;; `java.util.Map` keys, so a delete that arrived as one while the entry was stored under
;; the other would leave the deleted record answering `get-sentex` for the life of the
;; store.  The RAM store is immune because Clojure's own maps normalize integer hashing;
;; a `LinkedHashMap` does not, so the coercion is here.
(defn- ckey ^Long [id] (Long/valueOf (long id)))

(defn- str->strength [s] (some-> s keyword))
(defn- strength->str [k] (some-> k name))

;; ---- the store ----------------------------------------------------------

(defrecord SqliteRecordStore [^Connection conn ^ReentrantLock lock counter
                              ^Map sx-cache ^Map j-cache dur-id]
  p/RecordStore

  (next-id [_] (long (swap! counter inc)))

  (put-sentex [this sentex]
    (let [id (long (or (:id sentex) (p/next-id this)))
          sentex (assoc sentex :id id)]
      (locked lock
              (swap! counter max id)
              (jdbc/with-transaction [tx conn]
                ;; leave `premise` untouched on an overwrite (a re-put keeps a mark), but
                ;; the record and its authoritative strength are replaced.
                (jdbc/execute-one!
                 tx ["INSERT INTO record (id, kind, frame, premise, strength)
                VALUES (?, ?, ?, 0, ?)
                ON CONFLICT(id) DO UPDATE SET kind=excluded.kind,
                  frame=excluded.frame, strength=excluded.strength"
                     id kind-sentex (nippy/freeze sentex) (strength->str (:strength sentex))])
                (jdbc/execute-one!
                 tx ["INSERT INTO record_meta (k, v) VALUES ('high_water', ?)
                ON CONFLICT(k) DO UPDATE SET v=MAX(v, excluded.v)" id]))
              (.put sx-cache (ckey id) sentex))
      id))

  (get-sentex [_ id]
    ;; tallied on the protocol method rather than on the query below it, so the number
    ;; counts what a *caller* asked for and is the same event the RAM and disk stores
    ;; count — which is what makes the tallies comparable across backends
    (prof/record-fetch :sentex)
    (when (id-ok? id)
      (or (.get sx-cache (ckey id))
          (locked lock
                  (when-let [row (jdbc/execute-one!
                                  conn ["SELECT frame, strength FROM record
                                   WHERE id=? AND kind=?" id kind-sentex]
                                  lower-maps)]
                    ;; the column is authoritative — assoc it back so a mark-premise that
                    ;; only touched the column is reflected without a frame rewrite.
                    (let [sx (assoc (nippy/thaw (frame-of row)) :strength (str->strength (:strength row)))]
                      (.put sx-cache (ckey id) sx)
                      sx))))))

  (delete-sentex! [_ id]
    (when (id-ok? id)
      (locked lock
              (jdbc/with-transaction [tx conn]
                (jdbc/execute-one! tx ["DELETE FROM record WHERE id=? AND kind=?" id kind-sentex])
                (jdbc/execute-one! tx ["DELETE FROM record_provenance WHERE id=?" id]))
              (.remove sx-cache (ckey id))))
    nil)

  (put-justification [this justification]
    (let [id (long (or (:id justification) (p/next-id this)))
          justification (assoc justification :id id)]
      (locked lock
              (swap! counter max id)
              (jdbc/with-transaction [tx conn]
                (jdbc/execute-one!
                 tx ["INSERT INTO record (id, kind, frame, premise, strength)
                VALUES (?, ?, ?, 0, NULL)
                ON CONFLICT(id) DO UPDATE SET kind=excluded.kind, frame=excluded.frame"
                     id kind-justification (nippy/freeze justification)])
                (jdbc/execute-one!
                 tx ["INSERT INTO record_meta (k, v) VALUES ('high_water', ?)
                ON CONFLICT(k) DO UPDATE SET v=MAX(v, excluded.v)" id]))
              (.put j-cache (ckey id) justification))
      id))

  (get-justification [_ id]
    (prof/record-fetch :justification)
    (when (id-ok? id)
      (or (.get j-cache (ckey id))
          (locked lock
                  (when-let [row (jdbc/execute-one!
                                  conn ["SELECT frame FROM record WHERE id=? AND kind=?"
                                        id kind-justification]
                                  lower-maps)]
                    (let [d (nippy/thaw (frame-of row))]
                      (.put j-cache (ckey id) d)
                      d))))))

  (delete-justification! [_ id]
    (when (id-ok? id)
      (locked lock
              (jdbc/with-transaction [tx conn]
                (jdbc/execute-one! tx ["DELETE FROM record WHERE id=? AND kind=?" id kind-justification])
                (jdbc/execute-one! tx ["DELETE FROM record_provenance WHERE id=?" id]))
              (.remove j-cache (ckey id))))
    nil)

  ;; Guarded like the fetches — here and on `delete-provenance!`, `mark-premise` and
  ;; `unmark-premise!`: `id-ok?`'s contract is that a handle this store could never
  ;; have issued is answered quietly, not thrown.  The memory store makes these four
  ;; ops no-ops for one, and unguarded `(ckey id)` on an informant keyword is a
  ;; ClassCastException out of a door that must stay quiet.
  (put-provenance [_ id prov]
    (when (id-ok? id)
      (locked lock
              (jdbc/execute-one!
               conn ["INSERT INTO record_provenance (id, prov) VALUES (?, ?)
              ON CONFLICT(id) DO UPDATE SET prov=excluded.prov"
                     id (nippy/freeze prov)])))
    prov)

  (get-provenance [_ id]
    (prof/record-fetch :provenance)
    (when (id-ok? id)
      (locked lock
              (some-> (jdbc/execute-one!
                       conn ["SELECT prov FROM record_provenance WHERE id=?" id] lower-maps)
                      :prov nippy/thaw))))

  (delete-provenance! [_ id]
    (when (id-ok? id)
      (locked lock
              (jdbc/execute-one! conn ["DELETE FROM record_provenance WHERE id=?" id])))
    nil)

  (sentex-ids [_] (locked lock (id-roster conn kind-sentex nil)))

  (justification-ids [_] (locked lock (id-roster conn kind-justification nil)))

  (mark-premise [_ id strength]
    ;; guard on the sentex existing (no phantom premise): the UPDATE's WHERE is the
    ;; guard — it touches nothing when the row is absent.  Column-only, so no frame
    ;; rewrite; evict the cached record so the next fetch reflects the new strength.
    (when (id-ok? id)
      (locked lock
              (jdbc/execute-one!
               conn ["UPDATE record SET premise=1, strength=? WHERE id=? AND kind=?"
                     (strength->str (or strength :default)) id kind-sentex])
              (.remove sx-cache (ckey id))))
    nil)

  (unmark-premise! [_ id]
    (when (id-ok? id)
      (locked lock
              (jdbc/execute-one!
               conn ["UPDATE record SET premise=0, strength=NULL WHERE id=? AND kind=?"
                     id kind-sentex])
              (.remove sx-cache (ckey id))))
    nil)

  (premise-ids [_] (locked lock (id-roster conn kind-sentex " AND premise=1")))

  (premise-strength [_ id]
    (or (when (id-ok? id)
          (locked lock
                  (str->strength
                   (:strength (jdbc/execute-one!
                               conn ["SELECT strength FROM record WHERE id=? AND kind=?"
                                     id kind-sentex]
                               lower-maps)))))
        :default))

  (clear-records! [_]
    (locked lock
            (jdbc/with-transaction [tx conn]
              (jdbc/execute-one! tx ["DELETE FROM record"])
              (jdbc/execute-one! tx ["DELETE FROM record_provenance"])
              (jdbc/execute-one! tx ["DELETE FROM record_meta"]))
            (reset! counter 0)
            (.clear sx-cache)
            (.clear j-cache))
    nil)

  p/Tallying
  ;; The counts and the samples the engine asks an enumeration for without needing the
  ;; enumeration.  Embedded means no round trip, so the saving here is not latency: it is
  ;; that `open-kb` no longer scans the whole table and builds a roster out of it to
  ;; answer *how many* and *is there one*, twice, before the KB has answered anything.
  (sentex-tally [_]
    (locked lock (long (or (:c (jdbc/execute-one!
                                conn ["SELECT count(*) AS c FROM record WHERE kind=?"
                                      kind-sentex]
                                lower-maps))
                           0))))
  (justification-tally [_]
    (locked lock (long (or (:c (jdbc/execute-one!
                                conn ["SELECT count(*) AS c FROM record WHERE kind=?"
                                      kind-justification]
                                lower-maps))
                           0))))
  (a-sentex-id [_]
    (locked lock (:id (jdbc/execute-one!
                       conn ["SELECT id FROM record WHERE kind=? LIMIT 1" kind-sentex]
                       lower-maps))))
  (a-justification-id [_]
    (locked lock (:id (jdbc/execute-one!
                       conn ["SELECT id FROM record WHERE kind=? LIMIT 1" kind-justification]
                       lower-maps))))
  (a-premise-id [_]
    (locked lock (:id (jdbc/execute-one!
                       conn ["SELECT id FROM record WHERE kind=? AND premise=1 LIMIT 1"
                             kind-sentex]
                       lower-maps))))

  java.io.Closeable
  (close [_]
    (dur/deregister! @dur-id)
    (locked lock (.close conn))))

;; ---- the bulk ingest path -----------------------------------------------

(def ^:private default-batch
  "Rows per transaction on a bulk load.  A `put` is a transaction of its own — a commit
  and its WAL write per record — so this is the whole of what a sink here buys, and the
  number trades peak heap against how much a failure rolls back."
  10000)

(defn- batch-sink
  "A `RecordSink` that lands `batch` rows per transaction with one `execute-batch!`,
  instead of the transaction-per-record a `put` is.

  It is a **load and not an upsert**: a plain `INSERT`, so a handle the store already
  holds raises on the primary key rather than being overwritten silently.  That is the
  contract the seam's other implementation (`COPY`, in the Postgres adapter) has, and a
  bulk path that quietly replaced a record the caller did not know was there would be the
  worse answer on both.

  Nothing written here enters this store's read caches: a bulk load is a stream nobody is
  reading back, and filling the LRU with the tail of a corpus evicts what a later query
  wants for no gain."
  [store kind premises? batch]
  (when-not (pos? (long batch))
    (throw (ex-info (str "a bulk :batch must be a positive number of rows, got "
                         (pr-str batch))
                    {:type :bad-batch :batch batch})))
  (let [^Connection conn    (:conn store)
        ^ReentrantLock lock (:lock store)
        counter             (:counter store)
        pending             (ArrayList.)
        flush!  (fn []
                  (when-not (.isEmpty pending)
                    (let [rows (vec pending)
                          top  (long (reduce max 0 (map first rows)))]
                      (locked lock
                              (jdbc/with-transaction [tx conn]
                                (jdbc/execute-batch!
                                 tx "INSERT INTO record (id, kind, frame, premise, strength)
                                     VALUES (?, ?, ?, ?, ?)"
                                 rows {})
                                (jdbc/execute-one!
                                 tx ["INSERT INTO record_meta (k, v) VALUES ('high_water', ?)
                                      ON CONFLICT(k) DO UPDATE SET v=MAX(v, excluded.v)" top])))
                      (.clear pending))))]
    (reify
      p/RecordSink
      (write-record! [_ rec]
        (let [id  (long (or (:id rec) (long (swap! counter inc))))
              rec (assoc rec :id id)
              st  (when (= kind kind-sentex) (strength->str (:strength rec)))]
          (swap! counter max id)
          (.add pending [id kind (nippy/freeze rec)
                         (if (and premises? (some? st)) 1 0) st])
          (when (>= (.size pending) (long batch)) (flush!))
          id))

      java.io.Closeable
      (close [_] (flush!) nil))))

(defn- annotate-batch!
  "Run `sql` once per row of `rows` inside **one transaction**, with one `execute-batch!`.
  A statement here is a transaction of its own, so the transaction is what a bulk annotate
  buys — the same shape `batch-sink` gives a bulk load."
  [store sql rows]
  (when (seq rows)
    (let [^Connection conn    (:conn store)
          ^ReentrantLock lock (:lock store)]
      (locked lock
              (jdbc/with-transaction [tx conn]
                (jdbc/execute-batch! tx sql (vec rows) {})))))
  nil)

(extend-protocol p/BulkAnnotating
  SqliteRecordStore
  (mark-premise-batch [store id->strength]
    ;; the UPDATE's WHERE is the guard, exactly as it is one row at a time: a handle with
    ;; no sentex matches nothing.  The cached records are evicted either way — an eviction
    ;; for a handle that was not there costs a map lookup and cannot be wrong.
    (let [rows (into [] (map (fn [[id st]]
                               [(strength->str (or st :default)) id kind-sentex]))
                     id->strength)]
      (annotate-batch! store "UPDATE record SET premise=1, strength=? WHERE id=? AND kind=?"
                       rows)
      (let [^Map sx-cache (:sx-cache store)]
        (doseq [[id _] id->strength] (.remove sx-cache (ckey id)))))
    nil)
  (put-provenance-batch [store entries]
    (annotate-batch! store
                     "INSERT INTO record_provenance (id, prov) VALUES (?, ?)
                      ON CONFLICT(id) DO UPDATE SET prov=excluded.prov"
                     (into [] (map (fn [[id prov]] [id (nippy/freeze prov)])) entries))
    nil))

(extend-protocol p/BulkLoading
  SqliteRecordStore
  (open-sentex-sink [store {:keys [batch premises?] :or {batch default-batch premises? true}}]
    (batch-sink store kind-sentex premises? batch))
  (open-justification-sink [store {:keys [batch] :or {batch default-batch}}]
    (batch-sink store kind-justification false batch)))

;; ---- construction -------------------------------------------------------

(def ^:private default-cache-capacity 65536)

(defn- load-high-water
  "The counter's opening value: the larger of the persisted `high_water` and the
  live maximum handle, so a store that lost its last `high_water` write to a crash
  still never reissues a live handle, and one that deleted its max handle still
  never reissues *that*."
  ^long [^Connection conn]
  (let [hw   (:v (jdbc/execute-one! conn ["SELECT v FROM record_meta WHERE k='high_water'"] lower-maps))
        live (:m (jdbc/execute-one! conn ["SELECT MAX(id) AS m FROM record"] lower-maps))]
    (long (max (long (or hw 0)) (long (or live 0))))))

(defn sqlite-record-store
  "A durable `RecordStore` over the SQLite database `ds` (a next.jdbc db-spec or
  datasource — use a **file** db-spec, not `:memory:`, since the store keeps one
  connection and a fresh reopen must see the same database).

  Opens one connection for the store's lifetime under WAL + `synchronous=NORMAL`,
  creates the schema if absent, and loads the handle counter so recovery reopens
  where the last run stopped.  `:cache-capacity` sizes the per-kind fetch LRU
  (default 65536).  The store is `java.io.Closeable`; `close` releases the
  connection."
  ([ds] (sqlite-record-store ds {}))
  ([ds {:keys [cache-capacity] :or {cache-capacity default-cache-capacity}}]
   (let [^Connection conn (jdbc/get-connection ds)]
     (try
       (jdbc/execute-one! conn ["PRAGMA journal_mode=WAL"])
       (jdbc/execute-one! conn ["PRAGMA synchronous=NORMAL"])
       ;; SQLite is single-writer; wait out a lock rather than fail with SQLITE_BUSY.
       (jdbc/execute-one! conn ["PRAGMA busy_timeout=5000"])
       (ensure-schema! conn)
       (let [store (->SqliteRecordStore conn (ReentrantLock.) (atom (load-high-water conn))
                                        (lru cache-capacity) (lru cache-capacity) (atom nil))]
         (reset! (:dur-id store)
                 (dur/register!
                  {;; SQLite's own `synchronous=NORMAL` decides when a write reaches the
                   ;; platter, and there is no client-side buffer here for the engine to
                   ;; force — so the fsync tick is a no-op with a reason rather than an
                   ;; empty function.  Registered for the **close**: a JVM that exits
                   ;; without one still releases the connection and its WAL.
                   :fsync (fn [_] nil)
                   :close (fn [] (locked (:lock store) (.close ^Connection (:conn store))))
                   :label (str "sqlite-records " ds)}))
         store)
       ;; a throw between the borrow and `register!` means no `close` ever runs;
       ;; release the connection (and its WAL handle) rather than leaking one per
       ;; retried open of a corrupt or locked file.
       (catch Throwable t
         (.close conn)
         (throw t))))))
