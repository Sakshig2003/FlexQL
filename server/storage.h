#pragma once
#ifndef STORAGE_H
#define STORAGE_H

/*
 * storage.h – Persistent row-oriented storage engine
 *
 * Design:
 *   Each table lives in its own binary file: <data_dir>/<TABLE_NAME>.tbl
 *
 *   File layout:
 *     [SCHEMA HEADER]  – fixed 4096-byte block
 *       magic[4]       "FLXQ"
 *       version[4]     uint32_t = 1
 *       col_count[4]   uint32_t
 *       cols[MAX_COLS] column descriptors (name[64], type[8])
 *       row_size[4]    bytes per row (fixed-length)
 *       row_count[8]   total rows written (int64_t)
 *       deleted_count[8]
 *       reserved       padding to 4096
 *
 *     [DATA PAGES]  – contiguous rows after offset 4096
 *       Each row: [1-byte alive flag][row_size bytes of column data]
 *
 * Column data encoding (fixed width per type):
 *   INT / DECIMAL  → int64_t  (8 bytes)
 *   VARCHAR(n)     → n bytes  (NUL-padded, max MAX_VARCHAR_LEN)
 *   DATETIME       → int64_t  (8 bytes, Unix timestamp)
 *
 * Primary index:
 *   In-memory B+-tree (or sorted array) on the first column.
 *   Rebuilt on startup by scanning the data file.
 *
 * Cache:
 *   LRU page cache (configurable size) shared across tables.
 *   Each "page" = PAGE_SIZE bytes of the data file.
 */

#include <stdint.h>
#include <pthread.h>

#define STORAGE_MAGIC        "FLXQ"
#define STORAGE_VERSION      1
#define SCHEMA_HEADER_SIZE   4096
#define PAGE_SIZE            (64 * 1024)        /* 64 KB pages */
#define MAX_COLS             32
#define MAX_TABLE_NAME       64
#define MAX_COL_NAME         64
#define MAX_VARCHAR_LEN      256
#define MAX_CACHE_PAGES      4096               /* ~256 MB cache */

/* ── Column types ─────────────────────────────────────────────── */
typedef enum {
    COL_INT      = 1,
    COL_DECIMAL  = 2,
    COL_VARCHAR  = 3,
    COL_DATETIME = 4
} ColType;

typedef struct {
    char    name[MAX_COL_NAME];
    ColType type;
    int     varchar_len;    /* only meaningful for COL_VARCHAR */
    int     offset;         /* byte offset within a row */
    int     size;           /* byte size in row  */
} ColumnDef;

/* ── Schema ───────────────────────────────────────────────────── */
typedef struct {
    char      table_name[MAX_TABLE_NAME];
    int       col_count;
    ColumnDef cols[MAX_COLS];
    int       row_size;         /* total bytes per row */
} Schema;

/* ── Index node (skip-list or sorted dynamic array) ────────────── */
typedef struct IndexEntry {
    int64_t  key;       /* primary key value (first column, numeric) */
    int64_t  row_id;    /* 0-based row index in data file */
} IndexEntry;

typedef struct {
    IndexEntry *entries;
    int64_t     count;
    int64_t     capacity;
    int         is_sorted;
} PrimaryIndex;

/* ── LRU page cache ────────────────────────────────────────────── */
typedef struct CachePage {
    int64_t      page_id;
    char        *data;          /* PAGE_SIZE bytes */
    int          dirty;
    struct CachePage *lru_prev;
    struct CachePage *lru_next;
} CachePage;

typedef struct {
    CachePage  **map;           /* hash table: page_id → CachePage* */
    int          map_size;      /* must be power of 2 */
    CachePage   *lru_head;      /* most recently used */
    CachePage   *lru_tail;      /* least recently used */
    int          count;
    int          capacity;
    pthread_mutex_t lock;
} PageCache;

/* ── Table handle ──────────────────────────────────────────────── */
typedef struct {
    Schema       schema;
    int          fd;            /* data file fd */
    int64_t      row_count;     /* rows written (including deleted) */
    PrimaryIndex index;
    pthread_rwlock_t rw_lock;   /* table-level reader/writer lock */
} Table;

/* ── Database engine ───────────────────────────────────────────── */
typedef struct {
    char        data_dir[512];
    Table      *tables[128];
    char        table_names[128][MAX_TABLE_NAME];
    int         table_count;
    pthread_rwlock_t catalog_lock;  /* protects tables[] array */
    PageCache   cache;
} Database;

/* ── API ───────────────────────────────────────────────────────── */
int  db_open (const char *data_dir, Database **out);
void db_close(Database *db);

/* Returns FLEXQL_OK / FLEXQL_ERROR; sets *errmsg on error (caller frees) */
int  db_exec (Database *db, const char *sql,
              int (*callback)(void*, int, char**, char**),
              void *arg, char **errmsg);

#endif /* STORAGE_H */
