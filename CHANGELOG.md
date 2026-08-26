# Changelog

## 0.13.0 — 2026-08-25 — "the absent database, and the doors that stay quiet"

- **A source over a database no sink has written answers absent, and cleanup is
  the no-op it claims.** `read-manifest` on a file with no image tables answers
  nil — the seam's absent case — so a first-run `load-index!` reports
  `{:index :rebuild :reason :absent}` instead of throwing `no such table`, and
  `drop-image!` skips a table that is not there, as its docstring says.
  *Class:* Fix. *Migration:* none.

- **The four quiet doors are quiet.** `put-provenance`, `delete-provenance!`,
  `mark-premise` and `unmark-premise!` gate on `id-ok?` the way every fetch
  does, so a handle this store could never have issued — an informant keyword —
  is a no-op rather than a driver error or a `ClassCastException` out of the
  cache key. *Class:* Fix. *Migration:* none.

- **A failed open releases its connection.** `sqlite-record-store` and
  `sqlite-sink` close the connection they borrowed when construction throws
  past the borrow, so a retried open of a corrupt or locked file no longer
  leaks a handle and its WAL per attempt. *Class:* Fix. *Migration:* none.

## 0.12.0 — 2026-08-23 — "a KB image and a live store, in SQLite"

First release of the SQLite sibling (`com.vaelii/sqlite`) — an
**Apache-2.0** adapter on the SSPL engine. It depends on core; core never depends
on it. Requires core **0.12.0**: the record store answers seams that land there — the
bulk sink, the tallies, the two bare `BulkAnnotating` writes — and reads
`vaelii.impl.profile/record-fetch`, which lands in 0.11.0. The family releases in
lockstep, one version string across the engine, the plugin and the two adapters, which
is why a first release is numbered 0.12.0.

- **A bulk load is a transaction per batch, and `import!` takes it.** A `put` here is a
  transaction of its own — a commit and a WAL write per record — which is the whole of
  what a corpus load pays. The store answers core's `BulkLoading` seam
  (`open-sentex-sink` / `open-justification-sink`) with a sink that lands `:batch` rows
  per transaction in one `execute-batch!`, so an `import!` writes its records through it
  without naming this namespace. It is a **load and not an upsert** — a plain `INSERT`,
  so a handle the store already holds raises on the primary key rather than being
  replaced silently, which is the contract the Postgres adapter's `COPY` has. Through
  `import!` on a 30k-record dump, `{:belief? false}`: **7,444 → 16,572 records/s**,
  against core's `:disk` at 6,790. *Class:* Additive — a new optional capability; a
  caller that does not open a sink writes exactly as it did.

- **The premise marks and the provenance batch too.** A statement here is a transaction
  of its own, so core's `BulkAnnotating` lands as one `execute-batch!` inside one
  transaction for each — the same UPDATE and the same upsert the one-row ops run, with
  the same `WHERE` guarding against marking a handle that has no sentex. It is what an
  `import!` at `{:belief? true}` or `{:belief? :stored}` writes its marks and its
  provenance through. *Class:* Additive — a caller that does not ask for a batch writes
  exactly as it did.

- **The roster is compressed, and the tallies leave it alone.** The three enumerations
  answer core's `vaelii.impl.roster` rather than a `PersistentHashSet<Long>` — at 48–75
  bytes a handle the roster of a large store is the caller's heap, and handles arrive in
  the near-contiguous run `next-id` mints, which a bitmap holds at a fraction of a byte
  apiece and probes faster. The walk goes through `jdbc/plan` rather than `execute!`, so
  it never builds a result map per row on the way to discarding it. Beside it, core's
  `Tallying`: `open-kb` asks *how many records* and *is there one at all* twice before the
  KB has answered anything, and each is now a `count(*)` or a `LIMIT 1` instead of a table
  scan and a roster built out of it. *Class:* Additive — the enumerations answer the same
  handles, and the seam's contract is what says a set may be either shape.

- **A SQLite snapshot sink and source.** `vaelii.sqlite.snapshot` provides
  `sqlite-sink` (a `SnapshotSink` writing a KB image to a single-file database) and
  `sqlite-source` (a read-only `SnapshotSource` reading it back), over the engine's
  snapshot seam (`vaelii.impl.io.snapshot`). It holds the index projection today
  and any of the seam's named sections as they land. A section written through this
  sink reads back frame-identical through any source — file, memory, or SQLite —
  and a mismatched image is discarded and rebuilt, never trusted (the engine's
  shared `snapshot/decision`). *Class:* Additive — a new snapshot target; nothing in
  core changes.

- **A live embedded-SQLite record store, wired as the `:sqlite` backend.**
  `vaelii.sqlite.record_store` provides a `RecordStore` over a single-file
  `<dir>/records.sqlite` — the durable ground truth a KB reads and writes. Core
  resolves it lazily under `{:records :sqlite}` (the sugar `:sqlite`), so the engine
  carries no JDBC dependency and a KB that never asks for it loads none; off the
  classpath the backend refuses by name with the coordinate to add. A durable
  `:disk` index over `:sqlite` records is refused exactly as it is over `:memory`.
  *Class:* Additive — a new records backend; `:memory` and `:disk` are unchanged.
  Verified end to end through core (`test/vaelii/sqlite/backend_test.clj`).

  Three properties the fetch LRU in front of it owes, each with a test:

  - **A cache entry is keyed by the number, not by how it was boxed.** `Integer 5` and
    `Long 5` are equal numbers and different `java.util.Map` keys, so a delete whose
    handle arrives as one must still evict an entry stored under the other — otherwise a
    record the store no longer holds goes on answering `get-sentex` for the life of the
    store. Clojure's own maps normalize integer hashing, which is why the RAM and disk
    backends have nothing to do here.
  - **The three fetches are tallied** on the protocol method, as `RecordStore` requires,
    so `vaelii.impl.profile`'s `:fetches` counts what a caller asked for and is
    comparable with what the other backends report.
  - **The connection is released on JVM exit**, through the engine's durability daemon,
    so a process that exits without a `close!` still lets go of the file and its WAL.
    `fsync` is a no-op with a reason: SQLite's `synchronous=NORMAL` decides when a write
    lands, and there is no client-side buffer for the engine to force.

Pick by shape: a **snapshot** is a frozen image you re-export (backup, shipping a
corpus); a **record store** is an always-current backend the KB reads and writes.

Docs: this repo's `README.md`; the seam is [core's storage.md](https://github.com/vaelii/vaelii/blob/main/docs/storage.md).
