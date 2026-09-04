;; SPDX-License-Identifier: Apache-2.0
;; Copyright © 2026 Vaelii LLC and the Vaelii contributors.
(ns vaelii.sqlite.snapshot
  "A SQLite target for the engine's snapshot protocol
  (`vaelii.impl.io.snapshot`) — a `SnapshotSink` that writes a KB image to a
  single-file database and a `SnapshotSource` that reads it back.

  This is the **good SQLite lane**.  A live records/index store over a database
  pays a round trip per probe, two orders of magnitude off local, so the query
  path stays RAM-resident on every backend.  A **snapshot** is the opposite
  shape: an image is `O(sections)` bulk blob transfers, not `O(records)` tiny
  probes.  SQLite is where that shape is cheapest of all — the whole image is one
  file with no server to stand up, so \"put my KB in a file\" — for backup, for
  shipping a corpus to another host, for embedding a KB beside an app — is
  answered here, and the file is the artifact you copy.

  ## The shape

  The protocol is two ops each side (`write-section!`/`commit!`,
  `read-manifest`/`read-section`) and the image is a set of **named sections**
  plus a **manifest**.  Here:

  * a section is a row stream `(image, section, seq, chunk blob)` — each chunk a
    nippy-frozen batch of frames, so a section of a million entries is a hundred
    bulk inserts, not a million;
  * the whole image writes inside **one transaction** and the **manifest row
    commits it** — so a crash leaves no manifest, `read-manifest` returns nil,
    and the caller rebuilds.  That is the file sink's \"write `manifest.edn`
    last\" rule, but enforced by the database rather than by ordering: a failed
    write rolls back the sections too, it does not leave half of them;
  * the validity stamp (`kv/index-layout-version` and the records fingerprint)
    rides both a **column** (queryable) and the manifest blob, and the
    validate-or-discard check is the shared `snapshot/decision` — a mismatched
    image is discarded and the source rebuilds, never trusted.

  A section written through this sink reads back frame-identical through any
  source (file, memory, Postgres, SQLite) — the portability the protocol exists to
  give.

  ## The file

  Point the sink at a **file-backed** db-spec (`{:dbtype \"sqlite\" :dbname
  \"kb.db\"}` or a `jdbc:sqlite:…` url).  The sink, the source, and
  `ensure-schema!` each open their own connection, and for a file they all see
  the same database — the same multi-connection shape the Postgres sink has.  An
  in-memory `:memory:` spec gives **each connection its own database**, so the
  schema one opens is invisible to the next; use a file, or a shared-cache
  datasource, if you need memory.

  ## Boundary

  Apache-2.0, and an **adapter**: it depends on the SSPL engine's protocol and is
  never depended on by it.  It implements `vaelii.impl.io.snapshot`'s protocols,
  the same way a record-store adapter implements `vaelii.impl.protocols`."
  (:require [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [taoensso.nippy :as nippy]
            [vaelii.impl.io.snapshot :as snap])
  (:import [java.sql Connection]))

;; ---- schema -------------------------------------------------------------

(def ^:private section-ddl
  "CREATE TABLE IF NOT EXISTS vaelii_snapshot_section (
     image   text    NOT NULL,
     section text    NOT NULL,
     seq     integer NOT NULL,
     chunk   blob    NOT NULL,
     PRIMARY KEY (image, section, seq))")

(def ^:private manifest-ddl
  "CREATE TABLE IF NOT EXISTS vaelii_snapshot_manifest (
     image         text    PRIMARY KEY,
     format        integer NOT NULL,
     index_layout  integer NOT NULL,
     records_stamp text,
     entry_count   integer NOT NULL,
     manifest      blob    NOT NULL,
     committed_at  text    NOT NULL DEFAULT (datetime('now')))")

(defn ensure-schema!
  "Create the two image tables if they are absent.  Idempotent; runs in
  autocommit before a sink opens its transaction."
  [ds]
  (jdbc/execute-one! ds [section-ddl])
  (jdbc/execute-one! ds [manifest-ddl]))

;; ---- helpers ------------------------------------------------------------

(defn- sect-name
  "The section column value for a section key — a keyword or a string, coerced the
  same way on write and read so the two agree."
  ^String [section]
  (if (keyword? section) (name section) (str section)))

(defn- table-exists?
  "True when `table` exists on `ds` — the guard that keeps a read of a database no
  sink has written (and a `drop-image!` over one) answering quietly rather than
  throwing `no such table`."
  [ds ^String table]
  (some? (jdbc/execute-one!
          ds ["SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?" table])))

(defn- manifest-entry-count ^long [manifest]
  (reduce + 0 (map (comp long :count val) (:sections manifest))))

(defn- section-chunks
  "A lazy seq of a section's chunk `byte[]`s in `seq` order — one query per chunk,
  so a section streams back one chunk in memory at a time rather than realizing
  the whole image."
  [ds image section]
  (letfn [(step [^long seq-n]
            (lazy-seq
             (when-let [row (jdbc/execute-one!
                             ds
                             ["SELECT chunk FROM vaelii_snapshot_section
                               WHERE image = ? AND section = ? AND seq = ?"
                              image section seq-n]
                             {:builder-fn rs/as-unqualified-lower-maps})]
               (cons (:chunk row) (step (inc seq-n))))))]
    (step 0)))

;; ---- the sink -----------------------------------------------------------

(defrecord SqliteSink [^Connection conn image ^long chunk-size committed?]
  snap/SnapshotSink
  (write-section! [_ section frames]
    (let [sect (sect-name section)
          n    (volatile! 0)
          seq  (volatile! 0)]
      (doseq [batch (partition-all chunk-size frames)]
        (let [bv (vec batch)]
          (jdbc/execute-one!
           conn
           ["INSERT INTO vaelii_snapshot_section (image, section, seq, chunk)
             VALUES (?, ?, ?, ?)"
            image sect @seq (nippy/freeze bv)])
          (vswap! n + (count bv))
          (vswap! seq inc)))
      @n))
  (commit! [_ manifest]
    (jdbc/execute-one!
     conn
     ["INSERT INTO vaelii_snapshot_manifest
         (image, format, index_layout, records_stamp, entry_count, manifest)
       VALUES (?, ?, ?, ?, ?, ?)"
      image (:format manifest) (:index-layout manifest)
      (some-> (:records manifest) str)
      (manifest-entry-count manifest) (nippy/freeze manifest)])
    (.commit conn)
    (vreset! committed? true)
    nil)
  java.io.Closeable
  (close [_]
    (try
      (when-not @committed? (.rollback conn))
      (finally (.close conn)))))

(defn sqlite-sink
  "A `SnapshotSink` writing an `image` to SQLite over `ds` (a next.jdbc db-spec or
  datasource).  Use a **file-backed** `ds` — the sink, the source and
  `ensure-schema!` open separate connections, and only a file shares one database
  across them (see the ns docstring on `:memory:`).

  Opens one transaction; opening for an image clears any prior one, the same
  fresh-image rule the file sink gets by deleting its manifest.  **`commit!`
  commits the transaction; anything else must `close` the sink** — it is
  `java.io.Closeable`, and a close without a commit rolls the whole image back.
  Use `with-open`.

  `:chunk-size` frames per row (default 10000)."
  ([ds image] (sqlite-sink ds image {}))
  ([ds image {:keys [chunk-size] :or {chunk-size 10000}}]
   (ensure-schema! ds)
   (let [^Connection conn (jdbc/get-connection ds)]
     (try
       ;; SQLite is single-writer; wait out a concurrent writer's lock rather than
       ;; failing the image with SQLITE_BUSY the instant one is held.
       (jdbc/execute-one! conn ["PRAGMA busy_timeout = 5000"])
       (.setAutoCommit conn false)
       (jdbc/execute-one! conn ["DELETE FROM vaelii_snapshot_section  WHERE image = ?" image])
       (jdbc/execute-one! conn ["DELETE FROM vaelii_snapshot_manifest WHERE image = ?" image])
       (->SqliteSink conn image (long chunk-size) (volatile! false))
       ;; a throw between the borrow and the record means no `close` ever runs;
       ;; release the connection on the way out rather than leaking one per retry.
       (catch Throwable t
         (.close conn)
         (throw t))))))

;; ---- the source ---------------------------------------------------------

(defrecord SqliteSource [ds image]
  snap/SnapshotSource
  (read-manifest [_]
    ;; nil rather than `no such table` on a database no sink has touched: the protocol
    ;; reads "the manifest, or nil when the image is absent", and a first run over a
    ;; fresh file is exactly that absence.
    (when (table-exists? ds "vaelii_snapshot_manifest")
      (some-> (jdbc/execute-one!
               ds
               ["SELECT manifest FROM vaelii_snapshot_manifest WHERE image = ?" image]
               {:builder-fn rs/as-unqualified-lower-maps})
              :manifest
              nippy/thaw)))
  (read-section [_ section]
    (mapcat nippy/thaw (section-chunks ds image (sect-name section)))))

(defn sqlite-source
  "A read-only `SnapshotSource` over the `image` a `sqlite-sink` committed on `ds`.
  Reads nothing until asked — `read-manifest` for the validity check,
  `read-section` for the install."
  [ds image]
  (->SqliteSource ds image))

;; ---- cleanup ------------------------------------------------------------

(defn drop-image!
  "Remove an image's sections and manifest.  A no-op if the tables are absent."
  [ds image]
  (when (table-exists? ds "vaelii_snapshot_section")
    (jdbc/execute-one! ds ["DELETE FROM vaelii_snapshot_section  WHERE image = ?" image]))
  (when (table-exists? ds "vaelii_snapshot_manifest")
    (jdbc/execute-one! ds ["DELETE FROM vaelii_snapshot_manifest WHERE image = ?" image])))
