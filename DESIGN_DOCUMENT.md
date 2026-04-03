# FlexQL – Design Document

---

## Table of Contents

1. [System Architecture](#1-system-architecture)
2. [Storage Design](#2-storage-design)
3. [Indexing](#3-indexing)
4. [Caching Strategy](#4-caching-strategy)
5. [SQL Parser and Executor](#5-sql-parser-and-executor)
6. [Wire Protocol](#6-wire-protocol)
7. [Multithreading Design](#7-multithreading-design)
8. [Expiration Timestamps](#8-expiration-timestamps)
9. [Persistence and Fault Tolerance](#9-persistence-and-fault-tolerance)
10. [Supported SQL Commands](#10-supported-sql-commands)
11. [Performance Design Decisions](#11-performance-design-decisions)
12. [Where Data is Stored](#12-where-data-is-stored)

---

## 1. System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        CLIENT SIDE                          │
│                                                             │
│  ┌─────────────────┐        ┌──────────────────────────┐   │
│  │   flexql_repl   │        │   benchmark_flexql       │   │
│  │ (REPL terminal) │        │ (benchmark / unit tests) │   │
│  └────────┬────────┘        └────────────┬─────────────┘   │
│           │                              │                  │
│           └──────────┬───────────────────┘                  │
│                      │ uses flexql_client.c (libflexql)     │
│                      │ flexql_open / flexql_exec / etc.     │
└──────────────────────┼──────────────────────────────────────┘
                       │  TCP (port 9000)
                       │  Wire Protocol (binary framing)
┌──────────────────────┼──────────────────────────────────────┐
│                      │       SERVER SIDE                    │
│           ┌──────────▼──────────┐                          │
│           │   flexql_server     │ (main thread: accept)    │
│           │   server.c          │                          │
│           └──────────┬──────────┘                          │
│         ┌────────────┴──────────────┐                      │
│  Thread 1 (client A)    Thread N (client B)  ...           │
│         │                           │                      │
│         └─────────────┬─────────────┘                      │
│                       │ calls db_exec()                     │
│           ┌───────────▼──────────────┐                     │
│           │   storage.c              │                     │
│           │   SQL Parser + Executor  │                     │
│           │   LRU Page Cache         │                     │
│           │   Primary Index          │                     │
│           └───────────┬──────────────┘                     │
│                       │ pread / pwrite                      │
│           ┌───────────▼──────────────┐                     │
│           │   Disk: flexql_data/     │                     │
│           │   TABLENAME.tbl files    │                     │
│           └──────────────────────────┘                     │
└─────────────────────────────────────────────────────────────┘
```

The system has two independent components that communicate over TCP:

- **Server** (`flexql_server`): Manages all data — parsing, execution, storage, indexing, caching. Multithreaded; one OS thread per client connection.
- **Client Library** (`flexql_client.c`): Thin TCP wrapper exposing the four required C API functions. No SQL logic lives here.

---

## 2. Storage Design

### 2.1 Format Choice: Row-Major Heap File

Each table is stored as a single binary **heap file** on disk:

```
<data_dir>/<TABLE_NAME>.tbl
```

The layout is **row-oriented (row-major)**:

```
┌────────────────────────────────────────┐  Byte offset 0
│         Schema Header (4096 bytes)     │
│  magic[4]       = "FLXQ"               │
│  version[4]     = 1                    │
│  col_count[4]                          │
│  cols[32]       = {name, type,         │
│                    offset, size}       │
│  row_size[4]    = total bytes/row      │
│  row_count[8]   = rows appended        │
│  deleted_count[8]                      │
│  table_name[64]                        │
│  [padding to 4096]                     │
├────────────────────────────────────────┤  Byte offset 4096
│  Row 0:  [1-byte alive][...data...]    │
│  Row 1:  [1-byte alive][...data...]    │
│  Row 2:  [1-byte alive][...data...]    │
│  ...                                   │
└────────────────────────────────────────┘
```

**Why row-major?**

| Reason | Detail |
|--------|--------|
| Append performance | INSERT appends one contiguous block per row — sequential write, ideal for spinning disk and SSD alike |
| Row reconstruction | SELECT must reconstruct complete rows; row-major avoids seeking across multiple column files |
| Simplicity | Single file per table simplifies crash recovery and schema management |
| Benchmark workload | The benchmark is write-heavy (10M inserts) then read-heavy (SELECT queries); row-major suits both |

Column-major (columnar) storage would be better only for analytics over a single column of millions of rows — not the primary workload here.

### 2.2 Column Encoding (Fixed-Width)

All columns use fixed-width encoding for O(1) row access by index:

| SQL Type | Storage | Width |
|----------|---------|-------|
| `INT` | `int64_t` little-endian | 8 bytes |
| `DECIMAL` | `int64_t` little-endian | 8 bytes |
| `DATETIME` | `int64_t` Unix timestamp | 8 bytes |
| `VARCHAR(n)` | NUL-padded byte array | n bytes (max 256) |

Fixed-width rows mean: `row_offset = SCHEMA_HEADER_SIZE + row_id * (row_size + 1)`

This allows direct `pread`/`pwrite` by row index with no scanning.

### 2.3 Row Lifecycle

- **INSERT**: appends a row with alive=1 at offset `row_count * (row_size+1) + 4096`
- **DELETE FROM**: zeroes the file after the header (truncates to header size), resets `row_count=0`
- **DROP TABLE**: closes and `unlink()`s the `.tbl` file

---

## 3. Indexing

### 3.1 Structure: In-Memory Sorted Array with Binary Search

For each table with a numeric first column, the server maintains an **in-memory sorted array** of `(key, row_id)` pairs:

```c
typedef struct {
    int64_t key;      // value of first column
    int64_t row_id;   // 0-based row index in data file
} IndexEntry;
```

- **Lookup**: binary search → O(log n), ~25 comparisons for 10M rows
- **Insert**: append to unsorted buffer → O(1) amortised; sorted lazily on first query
- **Memory**: 16 bytes × 10M rows = 160 MB — well within RAM budget
- **Rebuild**: on server startup, scan the data file once to populate the index

### 3.2 Why Sorted Array?

| Structure | Lookup | Insert | Memory | Complexity |
|-----------|--------|--------|--------|-----------|
| Sorted array + binary search | O(log n) | O(1) amortised | 16n bytes | Low |
| B+ Tree | O(log n) | O(log n) | ~32n bytes | High |
| Hash table | O(1) avg | O(1) | ~24n bytes | Medium |

For this workload (bulk insert then queries), sorted array is the best tradeoff. The sort cost is paid once (lazily before first query), and binary search is fast enough for all realistic query rates.

### 3.3 Index Acceleration

When a WHERE clause targets the **first column with equality** (`WHERE ID = 42`):
1. Binary search finds the row_id directly
2. Only one `pread` is needed — O(log n) total

All other queries (range scans, other columns) fall back to a full sequential scan.

---

## 4. Caching Strategy

### 4.1 LRU Page Cache

A **Least Recently Used (LRU) page cache** is shared across all tables:

```
Cache size: 4096 pages × 64 KB/page = 256 MB
```

Pages are addressed by a composite key: `(file_descriptor, page_index)`.

```
┌─────────────────────────────────────────────────────┐
│                  LRU Page Cache                     │
│                                                     │
│  Hash map: key → CachePage*         (fast lookup)   │
│  Doubly-linked list: MRU ↔ ... ↔ LRU (eviction)    │
│                                                     │
│  On access: move to MRU head                        │
│  On miss:   load from disk, evict LRU if full       │
│  On evict:  write dirty page back to disk           │
└─────────────────────────────────────────────────────┘
```

### 4.2 Why LRU?

- **Repeated queries** on the same table get served from cache after first scan
- **Hot rows** (recently inserted / frequently queried) stay in cache
- **Benchmark pattern**: after 10M inserts, SELECT queries scan rows that were recently written — high cache hit rate

### 4.3 Cache vs. Other Strategies

| Strategy | Best For | Why not chosen |
|----------|----------|----------------|
| LRU | General workloads, temporal locality | ✅ Chosen |
| LFU | Highly skewed access | More complex, similar performance here |
| Predictive / prefetch | Sequential scans | Could be added as future work |
| No cache | Write-only | INSERT-heavy phases would still benefit |

### 4.4 Write-Through vs. Write-Back

The cache uses **write-back** (dirty pages written on eviction or flush):
- Reduces I/O for rows written then immediately read (common in benchmark)
- Dirty pages are flushed on table close and server shutdown for durability

---

## 5. SQL Parser and Executor

### 5.1 Lexer

A hand-written recursive-descent lexer tokenises the SQL string:
- Quoted strings (`'value'`) handled correctly
- Multi-character operators (`<=`, `>=`, `!=`, `<>`)
- `TABLE.COLUMN` dotted references
- Case-insensitive keywords

### 5.2 Execution Pipeline

```
SQL String
    │
    ▼
Lexer (tokenise)
    │
    ▼
Command dispatch (CREATE / DROP / INSERT / DELETE / SELECT)
    │
    ▼
Table catalog lookup (rwlock-protected)
    │
    ▼
Row I/O via page cache
    │
    ▼
Callback invocation (one call per result row)
```

### 5.3 Batch INSERT

The parser supports multi-value INSERT:
```sql
INSERT INTO T VALUES (1,'a',0),(2,'b',0),(3,'c',0);
```
All rows in a single statement are written in one pass — reducing round-trips from N to 1 for a batch of N rows.

---

## 6. Wire Protocol

Binary framing over TCP (all integers big-endian):

**Client → Server:**
```
[4 bytes: SQL length][N bytes: SQL text]
```

**Server → Client (repeated messages until Done/Error):**
```
[1 byte: type]['R'=Row | 'E'=Error | 'D'=Done]
[4 bytes: payload length]
[N bytes: payload]
```

Row payload format:
```
COL1\tCOL2\t...\nVAL1\tVAL2\t...
```
(tab-separated column names, newline, tab-separated values)

The `Done` message (type `'D'`, length 0) signals end of result set or successful non-SELECT execution.

---

## 7. Multithreading Design

### 7.1 Thread Model

```
Main thread: accept() loop
    │
    ├─► Thread 1  →  client_thread(fd_1)
    ├─► Thread 2  →  client_thread(fd_2)
    │   ...
    └─► Thread N  →  client_thread(fd_N)
```

Each accepted connection spawns a **detached pthread**. The thread owns its TCP socket, reads SQL statements in a loop, calls `db_exec()`, and streams results back until the client disconnects.

### 7.2 Locking Hierarchy

| Lock | Type | Protects | Granularity |
|------|------|----------|------------|
| `catalog_lock` | `pthread_rwlock_t` | `tables[]` array, table creation/deletion | Database-level |
| `table.rw_lock` | `pthread_rwlock_t` | Row data, row_count, index | Per-table |
| `cache.lock` | `pthread_mutex_t` | LRU list, hash map | Cache-global |

**Concurrent SELECTs**: Multiple threads can hold the table read-lock simultaneously → fully parallel reads.

**INSERT serialisation**: A single thread holds the table write-lock during insert → no torn rows.

**Lock order** (always acquired in this order to prevent deadlock):
1. `catalog_lock` (read) → table lookup
2. `table.rw_lock` (read/write) → row I/O
3. `cache.lock` (mutex) → page access

### 7.3 Concurrency Safety Properties

- No torn writes: row written atomically under write-lock
- No stale reads: readers acquire read-lock before scanning
- Index consistency: index updated inside write-lock, read inside read-lock (with lazy sort)
- No deadlock: consistent lock ordering; cache lock never held when acquiring table lock

---

## 8. Expiration Timestamps

The benchmark inserts an `EXPIRES_AT` column (Unix timestamp stored as `DECIMAL`/`int64_t`).

**Current implementation**: stored as a regular numeric column, queryable with WHERE:
```sql
SELECT * FROM BIG_USERS WHERE EXPIRES_AT > 1700000000;
```

**Future work**: a background reaper thread could periodically:
1. Scan all tables for rows where `EXPIRES_AT < time(NULL)`
2. Mark them as dead (alive byte = 0)
3. Update `deleted_count` in the schema header

This was intentionally left as application-level logic since the benchmark does not query on expiration.

---

## 9. Persistence and Fault Tolerance

### 9.1 Data Durability Guarantees

| Event | Behaviour |
|-------|-----------|
| Clean shutdown (`SIGINT`/`SIGTERM`) | All dirty cache pages flushed; `fsync()` called; `row_count` updated in header |
| Server crash mid-INSERT | Rows appended before crash are durable if OS page cache was flushed; `row_count` in header is the source of truth |
| Power loss | Rows beyond `row_count` in header are ignored on restart; no partial rows accepted |
| Restart | Server scans `.tbl` files in data dir, rebuilds schema + index from file |

### 9.2 Header Atomicity

The schema header is written with `pwrite()` which is atomic at the page level on Linux. The `row_count` field is updated after each INSERT batch — ensuring the header always reflects committed data.

### 9.3 Fresh Start (--reset)

When running the benchmark again against an existing data directory, restart the server with:
```bash
./flexql_server --reset --data-dir ./flexql_data
```
This deletes all `.tbl` files before opening the database, giving a guaranteed clean state.

---

## 10. Supported SQL Commands

| Command | Syntax | Notes |
|---------|--------|-------|
| `CREATE TABLE` | `CREATE TABLE name (col TYPE, ...)` | Types: INT, DECIMAL, VARCHAR(n), DATETIME |
| `CREATE TABLE IF NOT EXISTS` | `CREATE TABLE IF NOT EXISTS name (...)` | No-op if table exists |
| `DROP TABLE` | `DROP TABLE name` | Deletes file from disk |
| `DROP TABLE IF EXISTS` | `DROP TABLE IF EXISTS name` | No-op if not found |
| `INSERT` | `INSERT INTO name VALUES (v1,v2,...)` | Single or batch |
| `DELETE FROM` | `DELETE FROM name` | Truncates all rows |
| `SELECT *` | `SELECT * FROM name` | Full scan |
| `SELECT cols` | `SELECT c1,c2 FROM name` | Projection |
| `SELECT WHERE` | `SELECT ... WHERE col OP val` | Single condition; ops: =,!=,<,<=,>,>= |
| `INNER JOIN` | `SELECT ... FROM t1 INNER JOIN t2 ON t1.c=t2.c` | Two-table join |
| `JOIN + WHERE` | `... INNER JOIN ... WHERE col OP val` | Join with filter |

---

## 11. Performance Design Decisions

### 11.1 Insert Performance

| Technique | Impact |
|-----------|--------|
| Batch INSERT (`VALUES (…),(…)`) | Reduces N round-trips to 1; crucial for 10M rows |
| Write-back page cache | Absorbs small writes; coalesces disk I/O |
| Append-only writes | Sequential disk access; no seek penalty |
| Lazy index sort | Defers O(n log n) sort until first query |
| `TCP_NODELAY` | Eliminates Nagle algorithm delay for small packets |

### 11.2 Query Performance

| Technique | Impact |
|-----------|--------|
| Primary index (binary search) | O(log n) point lookup on first column |
| LRU page cache (256 MB) | Hot pages served from RAM; avoids disk reads |
| Fixed-width rows | O(1) row seek by index; no scanning for single-row lookup |
| Reader/writer locks | Parallel SELECT execution across multiple clients |

### 11.3 Memory Budget (10M rows, BIG_USERS table)

| Component | Size |
|-----------|------|
| Index (10M entries × 16 bytes) | ~160 MB |
| LRU page cache | ~256 MB |
| Row data on disk | ~5.2 GB (52 bytes/row × 10M) |
| Per-thread stack | ~8 MB per connection |

---

## 12. Where Data is Stored

### 12.1 On Disk

All persistent data lives in the **data directory** (default: `./flexql_data/`).

```
flexql_data/
├── BIG_USERS.tbl        ← created by benchmark insert (10M rows)
├── TEST_USERS.tbl       ← created by unit tests
└── TEST_ORDERS.tbl      ← created by unit tests
```

Each `.tbl` file is a binary heap file with the layout described in Section 2.

**To locate the data directory:**
```bash
ls ./flexql_data/          # default location
ls /path/to/custom/dir/    # if --data-dir was passed to server
```

**To inspect a table file:**
```bash
hexdump -C ./flexql_data/TEST_USERS.tbl | head -20   # raw hex
```

The first 4 bytes are always `46 4C 58 51` = ASCII `FLXQ` (the magic number).

### 12.2 In Memory (transient — lost on server restart)

| Structure | Location | Size |
|-----------|----------|------|
| Primary index arrays | Server heap (`PrimaryIndex.entries`) | 16 bytes × row_count per table |
| LRU page cache | Server heap (`PageCache`) | Up to 256 MB |
| Per-connection state | Thread stack + heap | Small |

### 12.3 Data Flow Diagram

```
Client                    Server                       Disk
  │                         │                            │
  │── INSERT INTO T ... ───►│                            │
  │                         │── pwrite row ─────────────►│
  │                         │   (via page cache)         │
  │                         │── update index in RAM      │
  │◄── DONE ────────────────│                            │
  │                         │                            │
  │── SELECT * FROM T ─────►│                            │
  │                         │── cache hit? ──────────────│
  │                         │◄── page data ──────────────│
  │                         │   (or from cache)          │
  │◄── ROW rows... ─────────│                            │
  │◄── DONE ────────────────│                            │
```

---

## Appendix: File Reference

| File | Purpose |
|------|---------|
| `include/flexql.h` | Public client API header |
| `include/protocol.h` | Wire protocol constants |
| `client/flexql_client.c` | Client library (TCP connection + framing) |
| `server/storage.h` | Storage engine types and function declarations |
| `server/storage.c` | SQL parser, executor, heap file I/O, LRU cache, index |
| `server/server.c` | TCP server, thread management, request dispatch |
| `repl/repl.c` | Interactive SQL terminal |
| `benchmark_flexql.cpp` | Benchmark and unit-test runner (provided) |
| `Makefile` | Build system |
| `README.md` | Quick-start guide |
| `DESIGN_DOCUMENT.md` | This document |
