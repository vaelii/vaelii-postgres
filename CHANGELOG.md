# Changelog

## 0.12.0 — 2026-08-23 — "the records and the image, in Postgres"

First release of the Postgres sibling (`com.vaelii/postgres`) — an
**Apache-2.0** adapter on the SSPL engine. It depends on core; core never depends
on it. Requires core **0.12.0**: the record store answers seams that land there — the
bulk sink, the tallies, the prefetch hint — and the `:pg` records axis is core's.
The family releases in lockstep, one version string across the engine, the plugin
and the two adapters, which is why a first release is numbered 0.12.0.

- **A Postgres record store: `:pg-memory` and `:pg-disk`.**
  `vaelii.postgres.record-store/pg-record-store` implements the engine's `RecordStore`
  seam over three tables — `vaelii_record (id, kind, frame, premise, strength)` with
  the whole record nippy-frozen and the assumption strength on its own authoritative
  column, plus provenance and a high-water meta row. `id` is **`bigint`**, which is the
  one decision here that would otherwise become an `ALTER TABLE` on 100M rows. A KB
  selects it with `{:backend :pg-memory :pg <db-spec>}`; `:schema` puts a KB's tables in
  a schema of its own. *Class:* Additive — a new records backend; nothing in the
  snapshot lane changes.

  - **`COPY` is what a server is for.** `copy-sentexes!` / `copy-justifications!` load
    through `COPY … FROM STDIN BINARY` at **95.8k records/s** on 20,000 records against
    a local server, where the per-record door manages 4.1k/s and core's own `:disk`
    store manages 52.7k/s. It is a load rather than an upsert — `COPY` has no `ON
    CONFLICT` — and a record's `:strength` rosters it as a premise there, which is what
    a dump's strength means and what a later `recover` reads.
  - **And `import!` reaches it, through core's `BulkLoading` seam.** The store answers
    `open-sentex-sink` / `open-justification-sink` with the copy stream itself — the
    connection and the `COPY` held open for the sink's lifetime, `:batch` rows buffered
    between writes into it — so a corpus loaded through the KB's own door takes the fast
    path without an application naming this namespace. `copy-sentexes!` /
    `copy-justifications!` are the same sink with a `doseq` around it. Through `import!`
    on a 30k-record dump, `{:belief? false}`: **2,784 → 17,882 records/s**, against
    core's `:disk` at 6,790 — a server-backed load is now faster than the local disk
    backend, and what is left is the engine's own per-frame work rather than this store.
    A sink is **all or nothing**, unlike a stream of puts: every row belongs to the one
    `COPY` statement, so a load that fails part way leaves the database as it found it.
  - **The premise marks and the provenance go in bulk too.** Core's `BulkAnnotating`
    lands here as one `UPDATE … FROM (VALUES …)` per 1,000 handles and one multi-row
    upsert per 1,000 provenance maps, in place of a round trip apiece — on a 10k-record
    `import!` at `{:belief? :stored}` that is 20,000 round trips gone and **3,038 →
    10,343 records/s** (3.4×, medians of three interleaved runs with the capability
    hidden and present). The batch mark `RETURNING`s the ids it actually touched: the
    one-row form reads its update count to decide whether the strength cache may be
    filled, and over a batch a count cannot say *which* rows matched — a cache filled
    past that would answer `premise-strength` for a handle `premise-ids` does not name.
  - **The round trip is the shape everything answers to.** An uncached point read is
    **282.6 µs** against the disk store's 0.26 µs warm; an LRU hit is **0.38 µs**. So
    the fetch cache is not an optimization, it is the backend's viability — and the
    premise-strength cache is filled by the `premise-ids` walk itself, since `recover`
    asks for every one of those strengths immediately after enumerating them.
  - **The roster is compressed, and the tallies are one row.** The three enumerations
    answer core's `vaelii.impl.roster` rather than a `PersistentHashSet<Long>`: at 48–75
    bytes a handle the roster of the corpus this backend exists for is gigabytes of the
    caller's heap before a record is fetched, and handles arrive from the cursor in the
    near-contiguous run a bitmap holds in a few run containers. Beside it, `Tallying` —
    `open-kb` asks *how many records* and *is there one at all* before the KB has answered
    anything, and spelled through the roster each is this table on the wire; here they are
    a `count(*)` and a `LIMIT 1`.

  - **The recovery walks are hinted too.** `recover` fetches every stored justification
    one handle at a time, which is the longest run of round trips this store sees and one
    the open pays on every start; `prefetch-justifications!` collapses it the same way the
    sentex half collapses a query's candidates. Measured on a 9,002-record / 6,000-
    justification KB: `reindex`+`recover` 1,656 ms → **740 ms**, against 506 ms for the
    same KB entirely in RAM.
  - **The cache never outlives what it caches.** A read is a miss, a round trip and an
    install, and the supported shape is one writer with a reader beside it — so a delete
    landing between the round trip and the install would be undone by it, and the deleted
    record would answer that reader for the life of the store. Fills take the read side of
    a read/write lock and mutations the write side, so reads stay concurrent (the reason
    the store is pooled at all) and a cache hit takes no lock. Beside it: a re-put
    invalidates the strength it just rewrote, a mark on an absent handle caches nothing,
    a strength upgrade lands whatever the cache capacity says, and a marked row whose
    strength column is NULL no longer throws out of every `recover` over that store.
  - **The enumerations stream** through a server-side cursor — autocommit off and a set
    fetch size — because `reindex` and `recover` walk all of them and buffering 100M
    rows in the driver is an `OutOfMemoryError`, not a slow query.
  - **One writer.** Belief lives in the writing process's RAM, so a second process on
    the same database hides the first's beliefs and deletes records it still believes.
    A database has no exclusive-open to take, so unlike the `:disk` backend this one
    cannot fail a second writer fast: it is a rule the operator keeps.

- **A Postgres snapshot sink and source.** `vaelii.postgres.snapshot` provides
  `pg-sink` (a `SnapshotSink` writing a KB image to a database) and `pg-source` (a
  read-only `SnapshotSource` reading it back), over the engine's snapshot seam
  (`vaelii.impl.io.snapshot`). It puts a KB image in Postgres — the index projection
  today, and any of the seam's named sections as they land. This is the Postgres lane
  with no round trip in it: an image is `O(sections)` bulk blob transfers, not
  `O(records)` tiny probes, so "put my KB in Postgres" — for backup, replication, PITR,
  or shipping a corpus to another host — is answered here whether or not the records
  live in a database at all. *Class:* Additive — a new snapshot target; nothing in core changes.

  Two properties the seam gives and a database sharpens:
  - **One transaction, manifest last.** The whole image writes inside a single
    transaction and the manifest row commits it, so a crash leaves no manifest and
    the source rebuilds — the file sink's "write the manifest last" rule enforced by
    the database.
  - **Validate or discard.** The records fingerprint and index-layout version ride a
    column and the manifest; a mismatched image is discarded and rebuilt, never
    trusted (the engine's shared `snapshot/decision`).

  A section written through this sink reads back frame-identical through any source —
  file, memory, or Postgres.

The **index** stays RAM-resident or local on every backend: a networked index would pay
a round trip per trie node, which is the wrong shape for the query path. `:pg-disk` is
the durable index over Postgres records, and those index files are **local** to the host
running the writer — they do not travel with the KB, and a second host rebuilds them.

Docs: this repo's `README.md`; the seam is [core's storage.md](https://github.com/vaelii/vaelii/blob/main/docs/storage.md).
