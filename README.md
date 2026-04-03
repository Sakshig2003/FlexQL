# FlexQL – A Simplified SQL Database System

A from-scratch client-server SQL database written in **C/C++** (no external DB libraries).

---

## Repository Structure

```
flexql/
├── include/
│   ├── flexql.h              # Public client API header
│   └── protocol.h            # Internal wire-protocol constants
├── server/
│   ├── server.c              # Multithreaded TCP server
│   ├── storage.h             # Storage engine header
│   └── storage.c             # SQL parser + executor + file I/O + cache + index
├── client/
│   └── flexql_client.c       # Client library (flexql_open/close/exec/free)
├── repl/
│   └── repl.c                # Interactive SQL terminal (REPL)
├── benchmark_flexql.cpp      # Provided benchmark / unit-test script
├── Makefile
├── README.md                 # This file
└── DESIGN_DOCUMENT.md        # Full architecture and design decisions
```

---

## Build

```bash
make          # builds server + repl + bench
make clean    # removes binaries and data dir
```

Produces:

| Binary | Purpose |
|--------|---------|
| `flexql_server` | Database server |
| `flexql_repl` | Interactive SQL client |
| `flexql_bench` | Benchmark / unit-test runner |

**Requirements:** GCC >= 7, G++ >= 7 (C++17), Linux/macOS with pthreads.

---

## Workflow – Step by Step

### Step 1 – Start the server

```bash
./flexql_server --port 9000 --data-dir ./flexql_data
```

Options:

| Flag | Default | Description |
|------|---------|-------------|
| --port PORT | 9000 | TCP port to listen on |
| --data-dir DIR | ./flexql_data | Directory for persistent .tbl files |
| --reset | off | Wipe all data on startup (use before re-running the benchmark) |

The server prints:
```
FlexQL server started. Data dir: ./flexql_data
Listening on 0.0.0.0:9000
```

### Step 2 – Run unit tests (fresh server)

In a second terminal:

```bash
./flexql_bench --unit-test
```

Expected output:
```
Connected to FlexQL

[[...Running Unit Tests...]]

[PASS] CREATE TABLE TEST_USERS (0 ms)
...
Unit Test Summary: 21/21 passed, 0 failed.
```

### Step 3 – Run the full insertion benchmark

```bash
./flexql_bench 10000000     # inserts 10 million rows, then runs unit tests
```

### Step 4 – Re-running the benchmark (IMPORTANT)

Because the database is **persistent**, tables created in Step 3 still exist on disk.
Running the benchmark again without resetting will cause CREATE TABLE to fail with
"Table already exists", which produces 4 test failures.

**Always restart the server with --reset before a new benchmark run:**

```bash
# Kill the running server first (Ctrl-C or kill PID)
./flexql_server --port 9000 --data-dir ./flexql_data --reset
```

--reset deletes all .tbl files in the data directory before opening the database.

### Step 5 – Interactive REPL

```bash
./flexql_repl --host 127.0.0.1 --port 9000
```

```sql
flexql> CREATE TABLE USERS(ID DECIMAL, NAME VARCHAR(64), BALANCE DECIMAL);
OK
flexql> INSERT INTO USERS VALUES (1, 'Alice', 1200);
OK
flexql> SELECT * FROM USERS WHERE BALANCE > 1000;
ID: 1 | NAME: Alice | BALANCE: 1200
OK
flexql> exit
```

---

## Where Is the Data Stored?

All persistent data lives in the data directory (default ./flexql_data/):

```
flexql_data/
├── BIG_USERS.tbl        <- 10M-row table created by benchmark
├── TEST_USERS.tbl       <- created by unit tests
└── TEST_ORDERS.tbl      <- created by unit tests
```

Each .tbl file is a binary heap file:

```
Bytes 0-4095      -> Schema header (magic "FLXQ", column definitions, row count)
Bytes 4096 onward -> Row data: [1-byte alive flag][fixed-width row data]
```

See DESIGN_DOCUMENT.md sections 2 and 12 for full details.

---

## Supported SQL

| Statement | Example |
|-----------|---------|
| CREATE TABLE | CREATE TABLE T(ID DECIMAL, NAME VARCHAR(64), TS DATETIME); |
| CREATE TABLE IF NOT EXISTS | CREATE TABLE IF NOT EXISTS T(ID DECIMAL); |
| DROP TABLE | DROP TABLE T; |
| DROP TABLE IF EXISTS | DROP TABLE IF EXISTS T; |
| INSERT (single) | INSERT INTO T VALUES (1, 'hello', 1700000000); |
| INSERT (batch) | INSERT INTO T VALUES (1,'a',0),(2,'b',0),(3,'c',0); |
| DELETE FROM | DELETE FROM T; -- removes all rows |
| SELECT * | SELECT * FROM T; |
| SELECT cols | SELECT ID, NAME FROM T; |
| SELECT WHERE | SELECT NAME FROM T WHERE BALANCE > 1000; |
| INNER JOIN | SELECT T1.NAME, T2.AMOUNT FROM T1 INNER JOIN T2 ON T1.ID = T2.USER_ID; |
| JOIN + WHERE | SELECT T1.NAME FROM T1 INNER JOIN T2 ON T1.ID=T2.UID WHERE T2.AMOUNT > 50; |

Supported column types: INT, DECIMAL, VARCHAR(n), DATETIME
WHERE operators: =, !=, <>, <, <=, >, >=

---

## Performance Tips

1. Increase INSERT_BATCH_SIZE in benchmark_flexql.cpp to 500-2000 for much higher throughput.
   Batching 1000 rows per SQL statement reduces round-trips from 10M to 10K.
2. The LRU page cache (256 MB) absorbs repeated reads automatically.
3. Point queries on the first column (WHERE ID = X) use the binary-search index, O(log n).

---

## Common Error and Fix

**Symptom:** [FAIL] CREATE TABLE TEST_USERS -> Table TEST_USERS already exists

**Cause:** Running the benchmark against an existing data directory from a previous run.

**Fix:** Restart the server with --reset:
```bash
./flexql_server --port 9000 --data-dir ./flexql_data --reset
```
