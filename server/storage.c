/*
 * storage.c – Persistent row-oriented storage engine for FlexQL
 */

#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

#include "storage.h"
#include "../include/flexql.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdarg.h>
#include <ctype.h>
#include <errno.h>
#include <time.h>
#include <math.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>

/* ══════════════════════════════════════════════════════════════════
   Utility / string helpers
   ══════════════════════════════════════════════════════════════════ */

static char *str_dup(const char *s)
{
    if (!s) return NULL;
    size_t n = strlen(s) + 1;
    char *d = malloc(n);
    if (d) memcpy(d, s, n);
    return d;
}

static void str_upper(char *s)
{
    for (; *s; s++) *s = (char)toupper((unsigned char)*s);
}

static void str_trim(char *s)
{
    /* leading */
    char *p = s;
    while (isspace((unsigned char)*p)) p++;
    if (p != s) memmove(s, p, strlen(p) + 1);
    /* trailing */
    int n = (int)strlen(s);
    while (n > 0 && isspace((unsigned char)s[n-1])) s[--n] = '\0';
}

static char *make_err(const char *fmt, ...)
{
    char buf[1024];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    return str_dup(buf);
}

/* ══════════════════════════════════════════════════════════════════
   Schema file I/O
   ══════════════════════════════════════════════════════════════════ */

#pragma pack(push,1)
typedef struct {
    char     magic[4];
    uint32_t version;
    uint32_t col_count;
    struct {
        char     name[64];
        uint32_t type;
        int32_t  varchar_len;
        int32_t  offset;
        int32_t  size;
    } cols[MAX_COLS];
    int32_t  row_size;
    int64_t  row_count;
    int64_t  deleted_count;
    char     table_name[MAX_TABLE_NAME];
    /* pad to SCHEMA_HEADER_SIZE */
} SchemaHeader;
#pragma pack(pop)

static int schema_write(int fd, const Schema *s, int64_t row_count)
{
    SchemaHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    memcpy(hdr.magic, STORAGE_MAGIC, 4);
    hdr.version   = STORAGE_VERSION;
    hdr.col_count = (uint32_t)s->col_count;
    hdr.row_size  = s->row_size;
    hdr.row_count = row_count;
    strncpy(hdr.table_name, s->table_name, MAX_TABLE_NAME - 1);
    for (int i = 0; i < s->col_count; i++) {
        strncpy(hdr.cols[i].name, s->cols[i].name, 63);
        hdr.cols[i].type        = (uint32_t)s->cols[i].type;
        hdr.cols[i].varchar_len = s->cols[i].varchar_len;
        hdr.cols[i].offset      = s->cols[i].offset;
        hdr.cols[i].size        = s->cols[i].size;
    }
    /* write header at offset 0 */
    if (pwrite(fd, &hdr, sizeof(hdr), 0) != sizeof(hdr)) return -1;
    /* pad to SCHEMA_HEADER_SIZE */
    char pad[SCHEMA_HEADER_SIZE];
    memset(pad, 0, SCHEMA_HEADER_SIZE);
    if (pwrite(fd, pad, SCHEMA_HEADER_SIZE - sizeof(hdr), sizeof(hdr))
            != SCHEMA_HEADER_SIZE - (int)sizeof(hdr)) return -1;
    return 0;
}

static int schema_read(int fd, Schema *s, int64_t *row_count_out)
{
    SchemaHeader hdr;
    if (pread(fd, &hdr, sizeof(hdr), 0) != (ssize_t)sizeof(hdr)) return -1;
    if (memcmp(hdr.magic, STORAGE_MAGIC, 4) != 0) return -1;
    s->col_count = (int)hdr.col_count;
    s->row_size  = hdr.row_size;
    strncpy(s->table_name, hdr.table_name, MAX_TABLE_NAME - 1);
    for (int i = 0; i < s->col_count; i++) {
        strncpy(s->cols[i].name, hdr.cols[i].name, MAX_COL_NAME - 1);
        s->cols[i].type        = (ColType)hdr.cols[i].type;
        s->cols[i].varchar_len = hdr.cols[i].varchar_len;
        s->cols[i].offset      = hdr.cols[i].offset;
        s->cols[i].size        = hdr.cols[i].size;
    }
    if (row_count_out) *row_count_out = hdr.row_count;
    return 0;
}

/* ══════════════════════════════════════════════════════════════════
   LRU Page Cache
   ══════════════════════════════════════════════════════════════════ */

static void cache_init(PageCache *c, int capacity)
{
    c->map_size = 1;
    while (c->map_size < capacity * 2) c->map_size <<= 1;
    c->map      = calloc((size_t)c->map_size, sizeof(CachePage*));
    c->lru_head = c->lru_tail = NULL;
    c->count    = 0;
    c->capacity = capacity;
    pthread_mutex_init(&c->lock, NULL);
}

static void cache_lru_remove(PageCache *c, CachePage *p)
{
    if (p->lru_prev) p->lru_prev->lru_next = p->lru_next;
    else             c->lru_head = p->lru_next;
    if (p->lru_next) p->lru_next->lru_prev = p->lru_prev;
    else             c->lru_tail = p->lru_prev;
    p->lru_prev = p->lru_next = NULL;
}

static void cache_lru_push_front(PageCache *c, CachePage *p)
{
    p->lru_prev = NULL;
    p->lru_next = c->lru_head;
    if (c->lru_head) c->lru_head->lru_prev = p;
    c->lru_head = p;
    if (!c->lru_tail) c->lru_tail = p;
}

/* Look up a (fd, page_id) -> treat fd*BIG+page_id as key */
static CachePage *cache_lookup(PageCache *c, int64_t key)
{
    int slot = (int)(key & (int64_t)(c->map_size - 1));
    /* linear probe */
    for (int i = 0; i < c->map_size; i++) {
        int idx = (slot + i) & (c->map_size - 1);
        if (!c->map[idx]) return NULL;
        if (c->map[idx]->page_id == key) return c->map[idx];
    }
    return NULL;
}

static void cache_insert_map(PageCache *c, CachePage *p)
{
    int slot = (int)(p->page_id & (int64_t)(c->map_size - 1));
    for (int i = 0; i < c->map_size; i++) {
        int idx = (slot + i) & (c->map_size - 1);
        if (!c->map[idx]) { c->map[idx] = p; return; }
    }
}

static void cache_remove_map(PageCache *c, int64_t key)
{
    int slot = (int)(key & (int64_t)(c->map_size - 1));
    for (int i = 0; i < c->map_size; i++) {
        int idx = (slot + i) & (c->map_size - 1);
        if (!c->map[idx]) return;
        if (c->map[idx]->page_id == key) {
            c->map[idx] = NULL;
            /* rehash tail */
            int j = (idx + 1) & (c->map_size - 1);
            while (c->map[j]) {
                CachePage *tmp = c->map[j];
                c->map[j] = NULL;
                cache_insert_map(c, tmp);
                j = (j + 1) & (c->map_size - 1);
            }
            return;
        }
    }
}

/* Read a page into cache (or return cached copy).
 * Returns pointer to data buffer (PAGE_SIZE bytes). NULL on error.
 * key = (int64_t)fd << 32 | page_index
 */
static char *cache_get_page(PageCache *c, int fd, int64_t page_idx, int load)
{
    int64_t key = ((int64_t)(uint32_t)fd << 32) | (int64_t)(uint32_t)page_idx;

    pthread_mutex_lock(&c->lock);
    CachePage *p = cache_lookup(c, key);
    if (p) {
        cache_lru_remove(c, p);
        cache_lru_push_front(c, p);
        pthread_mutex_unlock(&c->lock);
        return p->data;
    }

    /* evict LRU if full */
    if (c->count >= c->capacity) {
        CachePage *evict = c->lru_tail;
        if (evict) {
            if (evict->dirty) {
                int64_t off = SCHEMA_HEADER_SIZE + evict->page_id % (1LL<<32) * PAGE_SIZE;
                int efd = (int)(evict->page_id >> 32);
                pwrite(efd, evict->data, PAGE_SIZE, off);
            }
            cache_lru_remove(c, evict);
            cache_remove_map(c, evict->page_id);
            c->count--;
            free(evict->data);
            free(evict);
        }
    }

    p = calloc(1, sizeof(CachePage));
    p->data = malloc(PAGE_SIZE);
    p->page_id = key;
    p->dirty = 0;
    memset(p->data, 0, PAGE_SIZE);

    if (load) {
        off_t off = (off_t)SCHEMA_HEADER_SIZE + (off_t)page_idx * PAGE_SIZE;
        pread(fd, p->data, PAGE_SIZE, off);
    }

    cache_insert_map(c, p);
    cache_lru_push_front(c, p);
    c->count++;
    pthread_mutex_unlock(&c->lock);
    return p->data;
}

static void cache_mark_dirty(PageCache *c, int fd, int64_t page_idx)
{
    int64_t key = ((int64_t)(uint32_t)fd << 32) | (int64_t)(uint32_t)page_idx;
    pthread_mutex_lock(&c->lock);
    CachePage *p = cache_lookup(c, key);
    if (p) p->dirty = 1;
    pthread_mutex_unlock(&c->lock);
}

static void cache_flush_fd(PageCache *c, int fd)
{
    pthread_mutex_lock(&c->lock);
    CachePage *p = c->lru_head;
    while (p) {
        if ((int)(p->page_id >> 32) == fd && p->dirty) {
            int64_t pg = p->page_id & 0xFFFFFFFFLL;
            off_t off = (off_t)SCHEMA_HEADER_SIZE + (off_t)pg * PAGE_SIZE;
            pwrite(fd, p->data, PAGE_SIZE, off);
            p->dirty = 0;
        }
        p = p->lru_next;
    }
    pthread_mutex_unlock(&c->lock);
}

/* ══════════════════════════════════════════════════════════════════
   Row I/O (via page cache)
   ══════════════════════════════════════════════════════════════════ */

/* Row layout: 1 byte alive + row_size bytes */
#define ROW_OVERHEAD 1

static int64_t row_file_offset(const Schema *s, int64_t row_id)
{
    return (int64_t)SCHEMA_HEADER_SIZE + row_id * (int64_t)(s->row_size + ROW_OVERHEAD);
}

static int read_row(PageCache *cache, int fd, const Schema *s,
                    int64_t row_id, char *row_buf, int *alive)
{
    int64_t off  = row_file_offset(s, row_id);
    int64_t pg   = off / PAGE_SIZE;
    int      pg_off = (int)(off % PAGE_SIZE);
    int      total  = s->row_size + ROW_OVERHEAD;

    char *page = cache_get_page(cache, fd, pg, 1);
    if (!page) return -1;

    /* row might span two pages – unlikely with 64KB pages but handle it */
    if (pg_off + total <= PAGE_SIZE) {
        *alive = (unsigned char)page[pg_off];
        memcpy(row_buf, page + pg_off + ROW_OVERHEAD, (size_t)s->row_size);
    } else {
        /* cross-page: fall back to direct read */
        char tmp[1 + MAX_COLS * MAX_VARCHAR_LEN];
        if (pread(fd, tmp, (size_t)total, (off_t)off) < total) return -1;
        *alive = (unsigned char)tmp[0];
        memcpy(row_buf, tmp + ROW_OVERHEAD, (size_t)s->row_size);
    }
    return 0;
}

static int write_row(PageCache *cache, int fd, const Schema *s,
                     int64_t row_id, const char *row_buf, int alive)
{
    int64_t off    = row_file_offset(s, row_id);
    int64_t pg     = off / PAGE_SIZE;
    int      pg_off = (int)(off % PAGE_SIZE);
    int      total  = s->row_size + ROW_OVERHEAD;

    if (pg_off + total <= PAGE_SIZE) {
        char *page = cache_get_page(cache, fd, pg, 1);
        if (!page) return -1;
        page[pg_off] = (char)alive;
        memcpy(page + pg_off + ROW_OVERHEAD, row_buf, (size_t)s->row_size);
        cache_mark_dirty(cache, fd, pg);
    } else {
        /* cross-page: direct write */
        char tmp[1 + MAX_COLS * MAX_VARCHAR_LEN];
        tmp[0] = (char)alive;
        memcpy(tmp + ROW_OVERHEAD, row_buf, (size_t)s->row_size);
        if (pwrite(fd, tmp, (size_t)total, (off_t)off) < total) return -1;
    }
    return 0;
}

/* ══════════════════════════════════════════════════════════════════
   Primary Index (sorted array with binary search)
   ══════════════════════════════════════════════════════════════════ */

static void index_init(PrimaryIndex *idx)
{
    idx->entries   = NULL;
    idx->count     = 0;
    idx->capacity  = 0;
    idx->is_sorted = 1;
}

static void index_free(PrimaryIndex *idx)
{
    free(idx->entries);
    idx->entries = NULL;
    idx->count = idx->capacity = 0;
}

static void index_insert(PrimaryIndex *idx, int64_t key, int64_t row_id)
{
    if (idx->count >= idx->capacity) {
        int64_t ncap = idx->capacity ? idx->capacity * 2 : 4096;
        IndexEntry *ne = realloc(idx->entries, (size_t)ncap * sizeof(IndexEntry));
        if (!ne) return;
        idx->entries  = ne;
        idx->capacity = ncap;
    }
    idx->entries[idx->count].key    = key;
    idx->entries[idx->count].row_id = row_id;
    idx->count++;
    idx->is_sorted = 0;
}

static int cmp_index_entry(const void *a, const void *b)
{
    const IndexEntry *ia = a, *ib = b;
    if (ia->key < ib->key) return -1;
    if (ia->key > ib->key) return  1;
    return 0;
}

static void index_sort(PrimaryIndex *idx)
{
    if (!idx->is_sorted) {
        qsort(idx->entries, (size_t)idx->count, sizeof(IndexEntry), cmp_index_entry);
        idx->is_sorted = 1;
    }
}

/* returns row_id or -1 */
static int64_t index_lookup(PrimaryIndex *idx, int64_t key)
{
    index_sort(idx);
    int64_t lo = 0, hi = idx->count - 1;
    while (lo <= hi) {
        int64_t mid = (lo + hi) / 2;
        if (idx->entries[mid].key == key)  return idx->entries[mid].row_id;
        if (idx->entries[mid].key <  key)  lo = mid + 1;
        else                               hi = mid - 1;
    }
    return -1;
}

/* ══════════════════════════════════════════════════════════════════
   Value encoding / decoding
   ══════════════════════════════════════════════════════════════════ */

static void encode_int(char *dst, int64_t v)
{
    memcpy(dst, &v, 8);
}

static int64_t decode_int(const char *src)
{
    int64_t v;
    memcpy(&v, src, 8);
    return v;
}

static void encode_varchar(char *dst, const char *s, int max_len)
{
    memset(dst, 0, (size_t)max_len);
    if (s) strncpy(dst, s, (size_t)(max_len - 1));
}

/* Convert a string literal (SQL value) to row bytes for given column */
static int encode_value(const ColumnDef *col, const char *val, char *dst)
{
    switch (col->type) {
    case COL_INT:
    case COL_DECIMAL:
    case COL_DATETIME: {
        int64_t v = (int64_t)strtoll(val, NULL, 10);
        encode_int(dst, v);
        break;
    }
    case COL_VARCHAR:
        encode_varchar(dst, val, col->size);
        break;
    }
    return 0;
}

/* Convert row bytes to string for given column */
static void decode_value(const ColumnDef *col, const char *src, char *out, size_t outsz)
{
    switch (col->type) {
    case COL_INT:
    case COL_DECIMAL:
    case COL_DATETIME: {
        int64_t v = decode_int(src);
        snprintf(out, outsz, "%lld", (long long)v);
        break;
    }
    case COL_VARCHAR:
        snprintf(out, outsz, "%.*s", col->size, src);
        break;
    }
}

/* Compare a value in a row with a string literal (for WHERE clause) */
typedef enum { OP_EQ, OP_NE, OP_LT, OP_LE, OP_GT, OP_GE } CompOp;

static int compare_value(const ColumnDef *col, const char *row_data, CompOp op, const char *rhs)
{
    int cmp;
    switch (col->type) {
    case COL_INT:
    case COL_DECIMAL:
    case COL_DATETIME: {
        int64_t lv = decode_int(row_data + col->offset);
        int64_t rv = (int64_t)strtoll(rhs, NULL, 10);
        cmp = (lv < rv) ? -1 : (lv > rv) ? 1 : 0;
        break;
    }
    case COL_VARCHAR:
        cmp = strncmp(row_data + col->offset, rhs, (size_t)col->size);
        break;
    default:
        return 0;
    }
    switch (op) {
    case OP_EQ: return cmp == 0;
    case OP_NE: return cmp != 0;
    case OP_LT: return cmp < 0;
    case OP_LE: return cmp <= 0;
    case OP_GT: return cmp > 0;
    case OP_GE: return cmp >= 0;
    }
    return 0;
}

/* ══════════════════════════════════════════════════════════════════
   Table open / create / close
   ══════════════════════════════════════════════════════════════════ */

static Table *table_open(Database *db, const char *name)
{
    char path[768];
    snprintf(path, sizeof(path), "%s/%s.tbl", db->data_dir, name);

    int fd = open(path, O_RDWR | O_CREAT, 0644);
    if (fd < 0) return NULL;

    Table *t = calloc(1, sizeof(Table));
    if (!t) { close(fd); return NULL; }
    t->fd = fd;
    pthread_rwlock_init(&t->rw_lock, NULL);
    index_init(&t->index);

    /* check if file is new */
    struct stat st;
    fstat(fd, &st);
    if (st.st_size < SCHEMA_HEADER_SIZE) {
        /* new file – no rows yet */
        strncpy(t->schema.table_name, name, MAX_TABLE_NAME - 1);
        t->row_count = 0;
        return t;
    }

    /* load schema */
    if (schema_read(fd, &t->schema, &t->row_count) < 0) {
        close(fd);
        free(t);
        return NULL;
    }

    /* rebuild index from first column (numeric) */
    if (t->schema.col_count > 0 &&
        (t->schema.cols[0].type == COL_INT ||
         t->schema.cols[0].type == COL_DECIMAL ||
         t->schema.cols[0].type == COL_DATETIME)) {
        char *row_buf = malloc((size_t)t->schema.row_size);
        for (int64_t i = 0; i < t->row_count; i++) {
            int alive = 0;
            if (read_row(&db->cache, fd, &t->schema, i, row_buf, &alive) < 0) break;
            if (alive) {
                int64_t key = decode_int(row_buf + t->schema.cols[0].offset);
                index_insert(&t->index, key, i);
            }
        }
        free(row_buf);
        index_sort(&t->index);
    }

    return t;
}

static void table_close(Database *db, Table *t)
{
    if (!t) return;
    cache_flush_fd(&db->cache, t->fd);
    /* update row_count in header */
    SchemaHeader hdr;
    if (pread(t->fd, &hdr, sizeof(hdr), 0) == sizeof(hdr)) {
        hdr.row_count = t->row_count;
        pwrite(t->fd, &hdr, sizeof(hdr), 0);
    }
    fsync(t->fd);
    close(t->fd);
    index_free(&t->index);
    pthread_rwlock_destroy(&t->rw_lock);
    free(t);
}

/* ══════════════════════════════════════════════════════════════════
   Database open / close
   ══════════════════════════════════════════════════════════════════ */

int db_open(const char *data_dir, Database **out)
{
    mkdir(data_dir, 0755);

    Database *db = calloc(1, sizeof(Database));
    if (!db) return FLEXQL_ERROR;
    strncpy(db->data_dir, data_dir, sizeof(db->data_dir) - 1);
    pthread_rwlock_init(&db->catalog_lock, NULL);
    cache_init(&db->cache, MAX_CACHE_PAGES);

    /* load existing tables */
    DIR *dir = opendir(data_dir);
    if (dir) {
        struct dirent *de;
        while ((de = readdir(dir)) != NULL) {
            char *dot = strrchr(de->d_name, '.');
            if (!dot || strcmp(dot, ".tbl") != 0) continue;
            char tname[MAX_TABLE_NAME];
            int len = (int)(dot - de->d_name);
            if (len >= MAX_TABLE_NAME) continue;
            strncpy(tname, de->d_name, (size_t)len);
            tname[len] = '\0';
            if (db->table_count >= 128) break;
            Table *t = table_open(db, tname);
            if (t) {
                db->tables[db->table_count]     = t;
                strncpy(db->table_names[db->table_count], tname, MAX_TABLE_NAME - 1);
                db->table_count++;
            }
        }
        closedir(dir);
    }

    *out = db;
    return FLEXQL_OK;
}

void db_close(Database *db)
{
    if (!db) return;
    for (int i = 0; i < db->table_count; i++)
        table_close(db, db->tables[i]);
    pthread_rwlock_destroy(&db->catalog_lock);
    pthread_mutex_destroy(&db->cache.lock);
    free(db->cache.map);
    free(db);
}

/* ══════════════════════════════════════════════════════════════════
   SQL parser + executor
   ══════════════════════════════════════════════════════════════════ */

/* ── tokeniser ─────────────────────────────────────────────────── */

typedef struct {
    const char *sql;
    int         pos;
} Lexer;

static void lex_skip_ws(Lexer *l)
{
    while (l->sql[l->pos] && isspace((unsigned char)l->sql[l->pos])) l->pos++;
}

/* read next token (word, number, quoted string, single-char punctuation) */
static int lex_next(Lexer *l, char *tok, size_t max)
{
    lex_skip_ws(l);
    if (!l->sql[l->pos]) return 0;

    char c = l->sql[l->pos];

    /* quoted string */
    if (c == '\'') {
        l->pos++;
        size_t n = 0;
        while (l->sql[l->pos] && l->sql[l->pos] != '\'') {
            if (n + 1 < max) tok[n++] = l->sql[l->pos];
            l->pos++;
        }
        if (l->sql[l->pos] == '\'') l->pos++;
        tok[n] = '\0';
        return 1;
    }

    /* multi-char operators */
    if ((c == '<' || c == '>' || c == '!') && l->sql[l->pos+1] == '=') {
        tok[0] = c; tok[1] = '='; tok[2] = '\0';
        l->pos += 2;
        return 1;
    }

    /* single-char punctuation */
    if (c == '(' || c == ')' || c == ',' || c == ';' ||
        c == '=' || c == '<' || c == '>' || c == '.') {
        tok[0] = c; tok[1] = '\0';
        l->pos++;
        return 1;
    }

    /* word / number */
    size_t n = 0;
    while (l->sql[l->pos] && !isspace((unsigned char)l->sql[l->pos]) &&
           l->sql[l->pos] != '(' && l->sql[l->pos] != ')' &&
           l->sql[l->pos] != ',' && l->sql[l->pos] != ';' &&
           l->sql[l->pos] != '=' && l->sql[l->pos] != '<' &&
           l->sql[l->pos] != '>' && l->sql[l->pos] != '.') {
        if (n + 1 < max) tok[n++] = l->sql[l->pos];
        l->pos++;
    }
    tok[n] = '\0';
    return n > 0;
}

/* peek at next token without consuming */
static int lex_peek(Lexer *l, char *tok, size_t max)
{
    int saved = l->pos;
    int r = lex_next(l, tok, max);
    l->pos = saved;
    return r;
}

/* ── helper: find table by name ─────────────────────────────────── */

static Table *find_table(Database *db, const char *name)
{
    char uname[MAX_TABLE_NAME];
    strncpy(uname, name, MAX_TABLE_NAME - 1);
    uname[MAX_TABLE_NAME-1] = '\0';
    str_upper(uname);
    for (int i = 0; i < db->table_count; i++) {
        char tup[MAX_TABLE_NAME];
        strncpy(tup, db->table_names[i], MAX_TABLE_NAME - 1);
        tup[MAX_TABLE_NAME-1] = '\0';
        str_upper(tup);
        if (strcmp(tup, uname) == 0) return db->tables[i];
    }
    return NULL;
}

/* ── DROP TABLE ─────────────────────────────────────────────────── */

static int exec_drop_table(Database *db, Lexer *l, char **errmsg)
{
    char tok[256];
    if (!lex_next(l, tok, sizeof(tok)) || strcasecmp(tok, "TABLE") != 0) {
        *errmsg = make_err("Expected TABLE keyword"); return FLEXQL_ERROR;
    }

    /* optional IF EXISTS */
    int if_exists = 0;
    char peek[32];
    if (lex_peek(l, peek, sizeof(peek)) && strcasecmp(peek, "IF") == 0) {
        lex_next(l, tok, sizeof(tok));  /* consume IF */
        lex_next(l, tok, sizeof(tok));  /* consume EXISTS */
        if_exists = 1;
    }

    char tname[MAX_TABLE_NAME];
    if (!lex_next(l, tname, sizeof(tname))) {
        *errmsg = make_err("Expected table name"); return FLEXQL_ERROR;
    }
    str_upper(tname);

    pthread_rwlock_wrlock(&db->catalog_lock);
    int idx = -1;
    for (int i = 0; i < db->table_count; i++) {
        char tup[MAX_TABLE_NAME];
        strncpy(tup, db->table_names[i], MAX_TABLE_NAME - 1);
        tup[MAX_TABLE_NAME-1] = '\0';
        str_upper(tup);
        if (strcmp(tup, tname) == 0) { idx = i; break; }
    }

    if (idx < 0) {
        pthread_rwlock_unlock(&db->catalog_lock);
        if (if_exists) return FLEXQL_OK;
        *errmsg = make_err("Table %s not found", tname);
        return FLEXQL_ERROR;
    }

    Table *t = db->tables[idx];

    /* build file path */
    char path[768];
    snprintf(path, sizeof(path), "%s/%s.tbl", db->data_dir, db->table_names[idx]);

    /* flush & close the table */
    cache_flush_fd(&db->cache, t->fd);
    close(t->fd);
    index_free(&t->index);
    pthread_rwlock_destroy(&t->rw_lock);
    free(t);

    /* delete the file */
    unlink(path);

    /* remove from catalog by shifting */
    for (int i = idx; i < db->table_count - 1; i++) {
        db->tables[i] = db->tables[i + 1];
        memcpy(db->table_names[i], db->table_names[i + 1], MAX_TABLE_NAME);
    }
    db->tables[db->table_count - 1]         = NULL;
    db->table_names[db->table_count - 1][0] = '\0';
    db->table_count--;

    pthread_rwlock_unlock(&db->catalog_lock);
    return FLEXQL_OK;
}

/* ── DELETE FROM table (truncate all rows) ──────────────────────── */

static int exec_delete(Database *db, Lexer *l, char **errmsg)
{
    char tok[256];
    /* FROM */
    if (!lex_next(l, tok, sizeof(tok)) || strcasecmp(tok, "FROM") != 0) {
        *errmsg = make_err("Expected FROM"); return FLEXQL_ERROR;
    }
    char tname[MAX_TABLE_NAME];
    if (!lex_next(l, tname, sizeof(tname))) {
        *errmsg = make_err("Expected table name"); return FLEXQL_ERROR;
    }
    str_upper(tname);

    pthread_rwlock_rdlock(&db->catalog_lock);
    Table *t = find_table(db, tname);
    pthread_rwlock_unlock(&db->catalog_lock);
    if (!t) { *errmsg = make_err("Table %s not found", tname); return FLEXQL_ERROR; }

    pthread_rwlock_wrlock(&t->rw_lock);

    /* truncate file to just the header */
    ftruncate(t->fd, (off_t)SCHEMA_HEADER_SIZE);

    /* reset row count and index */
    t->row_count = 0;
    index_free(&t->index);
    index_init(&t->index);

    /* update persistent header */
    SchemaHeader hdr;
    if (pread(t->fd, &hdr, sizeof(hdr), 0) == (ssize_t)sizeof(hdr)) {
        hdr.row_count     = 0;
        hdr.deleted_count = 0;
        pwrite(t->fd, &hdr, sizeof(hdr), 0);
    }
    fsync(t->fd);

    pthread_rwlock_unlock(&t->rw_lock);
    return FLEXQL_OK;
}

/* ── CREATE TABLE ───────────────────────────────────────────────── */

static int exec_create_table(Database *db, Lexer *l, char **errmsg)
{
    char tok[256];
    if (!lex_next(l, tok, sizeof(tok)) || strcasecmp(tok, "TABLE") != 0) {
        *errmsg = make_err("Expected TABLE keyword"); return FLEXQL_ERROR;
    }

    /* optional IF NOT EXISTS */
    int if_not_exists = 0;
    {
        char peek2[32];
        if (lex_peek(l, peek2, sizeof(peek2)) && strcasecmp(peek2, "IF") == 0) {
            lex_next(l, tok, sizeof(tok));  /* consume IF   */
            lex_next(l, tok, sizeof(tok));  /* consume NOT  */
            lex_next(l, tok, sizeof(tok));  /* consume EXISTS */
            if_not_exists = 1;
        }
    }

    char tname[MAX_TABLE_NAME];
    if (!lex_next(l, tname, sizeof(tname))) {
        *errmsg = make_err("Expected table name"); return FLEXQL_ERROR;
    }
    str_upper(tname);

    /* check duplicate */
    pthread_rwlock_rdlock(&db->catalog_lock);
    int exists = (find_table(db, tname) != NULL);
    pthread_rwlock_unlock(&db->catalog_lock);
    if (exists) {
        if (if_not_exists) return FLEXQL_OK;
        *errmsg = make_err("Table %s already exists", tname);
        return FLEXQL_ERROR;
    }

    /* === OR_REPLACE path handled above; fall through to create === */

    if (!lex_next(l, tok, sizeof(tok)) || tok[0] != '(') {
        *errmsg = make_err("Expected '('"); return FLEXQL_ERROR;
    }

    Schema s;
    memset(&s, 0, sizeof(s));
    strncpy(s.table_name, tname, MAX_TABLE_NAME - 1);
    int offset = 0;

    for (;;) {
        char cname[MAX_COL_NAME], ctype[64];
        if (!lex_next(l, cname, sizeof(cname))) break;
        if (cname[0] == ')') break;
        if (cname[0] == ',') {
            if (!lex_next(l, cname, sizeof(cname))) break;
            if (cname[0] == ')') break;
        }
        if (!lex_next(l, ctype, sizeof(ctype))) break;
        str_upper(ctype);

        if (s.col_count >= MAX_COLS) {
            *errmsg = make_err("Too many columns"); return FLEXQL_ERROR;
        }
        ColumnDef *cd = &s.cols[s.col_count];
        strncpy(cd->name, cname, MAX_COL_NAME - 1);
        str_upper(cd->name);
        cd->offset = offset;

        if (strcmp(ctype, "INT") == 0 || strcmp(ctype, "DECIMAL") == 0) {
            cd->type = (strcmp(ctype, "INT") == 0) ? COL_INT : COL_DECIMAL;
            cd->size = 8;
        } else if (strcmp(ctype, "DATETIME") == 0) {
            cd->type = COL_DATETIME;
            cd->size = 8;
        } else if (strncmp(ctype, "VARCHAR", 7) == 0) {
            cd->type = COL_VARCHAR;
            /* parse (n) */
            int vlen = MAX_VARCHAR_LEN;
            char peek[32];
            if (lex_peek(l, peek, sizeof(peek)) && peek[0] == '(') {
                lex_next(l, peek, sizeof(peek)); /* consume ( */
                char nstr[32];
                lex_next(l, nstr, sizeof(nstr));
                vlen = atoi(nstr);
                lex_next(l, peek, sizeof(peek)); /* consume ) */
            }
            if (vlen <= 0 || vlen > MAX_VARCHAR_LEN) vlen = MAX_VARCHAR_LEN;
            cd->varchar_len = vlen;
            cd->size = vlen;
        } else {
            *errmsg = make_err("Unknown type %s", ctype); return FLEXQL_ERROR;
        }

        offset += cd->size;
        s.col_count++;
    }

    if (s.col_count == 0) {
        *errmsg = make_err("No columns defined"); return FLEXQL_ERROR;
    }
    s.row_size = offset;

    /* create table file */
    pthread_rwlock_wrlock(&db->catalog_lock);
    Table *t = table_open(db, tname);
    if (!t) {
        pthread_rwlock_unlock(&db->catalog_lock);
        *errmsg = make_err("Cannot create table file"); return FLEXQL_ERROR;
    }
    t->schema    = s;
    t->row_count = 0;

    if (schema_write(t->fd, &s, 0) < 0) {
        table_close(db, t);
        pthread_rwlock_unlock(&db->catalog_lock);
        *errmsg = make_err("Cannot write schema"); return FLEXQL_ERROR;
    }

    if (db->table_count < 128) {
        db->tables[db->table_count] = t;
        strncpy(db->table_names[db->table_count], tname, MAX_TABLE_NAME - 1);
        db->table_count++;
    }
    pthread_rwlock_unlock(&db->catalog_lock);
    return FLEXQL_OK;
}

/* ── INSERT ─────────────────────────────────────────────────────── */

static int exec_insert(Database *db, Lexer *l, char **errmsg)
{
    char tok[256];
    /* INTO */
    if (!lex_next(l, tok, sizeof(tok)) || strcasecmp(tok, "INTO") != 0) {
        *errmsg = make_err("Expected INTO"); return FLEXQL_ERROR;
    }
    char tname[MAX_TABLE_NAME];
    if (!lex_next(l, tname, sizeof(tname))) {
        *errmsg = make_err("Expected table name"); return FLEXQL_ERROR;
    }
    str_upper(tname);

    /* VALUES */
    if (!lex_next(l, tok, sizeof(tok)) || strcasecmp(tok, "VALUES") != 0) {
        *errmsg = make_err("Expected VALUES"); return FLEXQL_ERROR;
    }

    pthread_rwlock_rdlock(&db->catalog_lock);
    Table *t = find_table(db, tname);
    pthread_rwlock_unlock(&db->catalog_lock);
    if (!t) { *errmsg = make_err("Table %s not found", tname); return FLEXQL_ERROR; }

    /* support batch: VALUES (...), (...), ... */
    char *row_buf = malloc((size_t)t->schema.row_size);
    if (!row_buf) { *errmsg = make_err("OOM"); return FLEXQL_ERROR; }

    pthread_rwlock_wrlock(&t->rw_lock);

    for (;;) {
        char peek[8];
        if (!lex_peek(l, peek, sizeof(peek)) || peek[0] != '(') break;
        lex_next(l, tok, sizeof(tok)); /* consume ( */

        memset(row_buf, 0, (size_t)t->schema.row_size);
        for (int c = 0; c < t->schema.col_count; c++) {
            if (c > 0) lex_next(l, tok, sizeof(tok)); /* consume , */
            char val[MAX_VARCHAR_LEN + 16];
            if (!lex_next(l, val, sizeof(val))) break;
            encode_value(&t->schema.cols[c], val,
                         row_buf + t->schema.cols[c].offset);
        }
        lex_next(l, tok, sizeof(tok)); /* consume ) */

        int64_t row_id = t->row_count;
        write_row(&db->cache, t->fd, &t->schema, row_id, row_buf, 1);
        t->row_count++;

        /* update index */
        if (t->schema.col_count > 0 &&
            (t->schema.cols[0].type == COL_INT ||
             t->schema.cols[0].type == COL_DECIMAL ||
             t->schema.cols[0].type == COL_DATETIME)) {
            int64_t key = decode_int(row_buf + t->schema.cols[0].offset);
            index_insert(&t->index, key, row_id);
        }

        /* check for , or ; */
        if (!lex_peek(l, peek, sizeof(peek)) || peek[0] != ',') break;
        lex_next(l, tok, sizeof(tok)); /* consume , */
    }

    /* update persistent row_count */
    {
        SchemaHeader hdr;
        if (pread(t->fd, &hdr, sizeof(hdr), 0) == sizeof(hdr)) {
            hdr.row_count = t->row_count;
            pwrite(t->fd, &hdr, sizeof(hdr), 0);
        }
    }

    pthread_rwlock_unlock(&t->rw_lock);
    free(row_buf);
    return FLEXQL_OK;
}

/* ── SELECT helpers ─────────────────────────────────────────────── */

typedef struct {
    char  col[MAX_COL_NAME];
    CompOp op;
    char  val[MAX_VARCHAR_LEN + 16];
    int   has_where;
} WhereClause;

static CompOp parse_op(const char *s)
{
    if (strcmp(s, "=")  == 0) return OP_EQ;
    if (strcmp(s, "!=") == 0 || strcmp(s, "<>") == 0) return OP_NE;
    if (strcmp(s, "<")  == 0) return OP_LT;
    if (strcmp(s, "<=") == 0) return OP_LE;
    if (strcmp(s, ">")  == 0) return OP_GT;
    if (strcmp(s, ">=") == 0) return OP_GE;
    return OP_EQ;
}

/* find column index in schema (case-insensitive, supports TABLE.COL) */
static int find_col(const Schema *s, const char *name)
{
    /* strip optional TABLE. prefix */
    const char *dot = strchr(name, '.');
    const char *cname = dot ? dot + 1 : name;
    char uname[MAX_COL_NAME];
    strncpy(uname, cname, MAX_COL_NAME - 1);
    uname[MAX_COL_NAME-1] = '\0';
    str_upper(uname);
    for (int i = 0; i < s->col_count; i++) {
        char uc[MAX_COL_NAME];
        strncpy(uc, s->cols[i].name, MAX_COL_NAME - 1);
        uc[MAX_COL_NAME-1] = '\0';
        str_upper(uc);
        if (strcmp(uc, uname) == 0) return i;
    }
    return -1;
}

/* ── SELECT (single table) ──────────────────────────────────────── */

static int exec_select(Database *db, Lexer *l,
                       int (*callback)(void*,int,char**,char**),
                       void *arg, char **errmsg)
{
    char tok[512];

    /* parse column list */
    char sel_cols[MAX_COLS][MAX_COL_NAME];
    int  sel_count = 0;
    int  select_star = 0;

    /* could be * or col list */
    lex_peek(l, tok, sizeof(tok));
    if (strcmp(tok, "*") == 0) {
        lex_next(l, tok, sizeof(tok));
        select_star = 1;
    } else {
        for (;;) {
            char ctok[MAX_COL_NAME + MAX_TABLE_NAME + 4];
            if (!lex_next(l, ctok, sizeof(ctok))) break;
            /* peek: might have TABLE.COL form – check next token for '.' */
            char peek2[8];
            if (lex_peek(l, peek2, sizeof(peek2)) && peek2[0] == '.') {
                lex_next(l, peek2, sizeof(peek2)); /* consume . */
                char col2[MAX_COL_NAME];
                lex_next(l, col2, sizeof(col2));
                /* build TABLE.COL */
                snprintf(ctok, sizeof(ctok), "%s.%s", ctok, col2);
            }
            if (sel_count < MAX_COLS)
                strncpy(sel_cols[sel_count++], ctok, MAX_COL_NAME - 1);
            /* peek for , or FROM */
            if (!lex_peek(l, peek2, sizeof(peek2)) || peek2[0] != ',') break;
            lex_next(l, peek2, sizeof(peek2));
        }
    }

    /* FROM */
    if (!lex_next(l, tok, sizeof(tok)) || strcasecmp(tok, "FROM") != 0) {
        *errmsg = make_err("Expected FROM"); return FLEXQL_ERROR;
    }

    char tname[MAX_TABLE_NAME];
    if (!lex_next(l, tname, sizeof(tname))) {
        *errmsg = make_err("Expected table name"); return FLEXQL_ERROR;
    }
    str_upper(tname);

    /* check for INNER JOIN */
    char peek[32];
    lex_peek(l, peek, sizeof(peek));
    if (strcasecmp(peek, "INNER") == 0) {
        /* rewind: let exec_select_join handle */
        /* undo peek – just pass through */
        /* we need to re-assemble – easier to handle in caller;
           here we detect INNER JOIN and dispatch */
        lex_next(l, tok, sizeof(tok)); /* consume INNER */
        lex_next(l, tok, sizeof(tok)); /* consume JOIN */

        char tname2[MAX_TABLE_NAME];
        if (!lex_next(l, tname2, sizeof(tname2))) {
            *errmsg = make_err("Expected second table name"); return FLEXQL_ERROR;
        }
        str_upper(tname2);

        /* ON */
        if (!lex_next(l, tok, sizeof(tok)) || strcasecmp(tok, "ON") != 0) {
            *errmsg = make_err("Expected ON"); return FLEXQL_ERROR;
        }
        /* left.col = right.col */
        char lhs_tok[128], op_tok[8], rhs_tok[128];
        lex_next(l, lhs_tok, sizeof(lhs_tok));
        /* check for table.col form */
        char dot2[4];
        if (lex_peek(l, dot2, sizeof(dot2)) && dot2[0] == '.') {
            lex_next(l, dot2, sizeof(dot2));
            char col2[MAX_COL_NAME];
            lex_next(l, col2, sizeof(col2));
            snprintf(lhs_tok, sizeof(lhs_tok), "%s.%s", lhs_tok, col2);
        }
        lex_next(l, op_tok, sizeof(op_tok));
        lex_next(l, rhs_tok, sizeof(rhs_tok));
        if (lex_peek(l, dot2, sizeof(dot2)) && dot2[0] == '.') {
            lex_next(l, dot2, sizeof(dot2));
            char col2[MAX_COL_NAME];
            lex_next(l, col2, sizeof(col2));
            snprintf(rhs_tok, sizeof(rhs_tok), "%s.%s", rhs_tok, col2);
        }

        /* optional WHERE */
        WhereClause wc; memset(&wc, 0, sizeof(wc));
        if (lex_peek(l, peek, sizeof(peek)) && strcasecmp(peek, "WHERE") == 0) {
            lex_next(l, tok, sizeof(tok));
            char wtok[MAX_COL_NAME + MAX_TABLE_NAME + 4];
            lex_next(l, wtok, sizeof(wtok));
            if (lex_peek(l, dot2, sizeof(dot2)) && dot2[0] == '.') {
                lex_next(l, dot2, sizeof(dot2));
                char col2[MAX_COL_NAME];
                lex_next(l, col2, sizeof(col2));
                snprintf(wtok, sizeof(wtok), "%s.%s", wtok, col2);
            }
            char wop[8], wval[MAX_VARCHAR_LEN+16];
            lex_next(l, wop, sizeof(wop));
            lex_next(l, wval, sizeof(wval));
            strncpy(wc.col, wtok, MAX_COL_NAME-1);
            wc.op = parse_op(wop);
            strncpy(wc.val, wval, sizeof(wc.val)-1);
            wc.has_where = 1;
        }

        /* ── execute INNER JOIN ─── */
        pthread_rwlock_rdlock(&db->catalog_lock);
        Table *t1 = find_table(db, tname);
        Table *t2 = find_table(db, tname2);
        pthread_rwlock_unlock(&db->catalog_lock);
        if (!t1) { *errmsg = make_err("Table %s not found", tname); return FLEXQL_ERROR; }
        if (!t2) { *errmsg = make_err("Table %s not found", tname2); return FLEXQL_ERROR; }

        /* resolve join columns */
        int j1 = find_col(&t1->schema, lhs_tok);
        int j2 = find_col(&t2->schema, rhs_tok);
        if (j1 < 0) { j1 = find_col(&t1->schema, rhs_tok); j2 = find_col(&t2->schema, lhs_tok); }
        if (j1 < 0 || j2 < 0) {
            *errmsg = make_err("Cannot resolve join columns %s / %s", lhs_tok, rhs_tok);
            return FLEXQL_ERROR;
        }

        /* build combined schema for output */
        int total_cols = t1->schema.col_count + t2->schema.col_count;
        char **col_names = malloc((size_t)(total_cols + 1) * sizeof(char*));
        for (int i = 0; i < t1->schema.col_count; i++)
            col_names[i] = t1->schema.cols[i].name;
        for (int i = 0; i < t2->schema.col_count; i++)
            col_names[t1->schema.col_count + i] = t2->schema.cols[i].name;
        col_names[total_cols] = NULL;

        /* filter to selected columns */
        int out_idx[MAX_COLS * 2];
        int out_count = 0;
        if (select_star) {
            for (int i = 0; i < total_cols; i++) out_idx[out_count++] = i;
        } else {
            for (int s2 = 0; s2 < sel_count; s2++) {
                int found = -1;
                for (int i = 0; i < t1->schema.col_count; i++) {
                    if (find_col(&t1->schema, sel_cols[s2]) == i) { found = i; break; }
                }
                if (found < 0) {
                    for (int i = 0; i < t2->schema.col_count; i++) {
                        if (find_col(&t2->schema, sel_cols[s2]) == i) {
                            found = t1->schema.col_count + i; break;
                        }
                    }
                }
                if (found >= 0) out_idx[out_count++] = found;
                else { *errmsg = make_err("Column %s not found", sel_cols[s2]); free(col_names); return FLEXQL_ERROR; }
            }
        }

        char **out_col_names = malloc((size_t)(out_count + 1) * sizeof(char*));
        for (int i = 0; i < out_count; i++) out_col_names[i] = col_names[out_idx[i]];
        out_col_names[out_count] = NULL;

        char *row1 = malloc((size_t)t1->schema.row_size);
        char *row2 = malloc((size_t)t2->schema.row_size);
        char **vals = malloc((size_t)(out_count + 1) * sizeof(char*));
        char valbufs[MAX_COLS * 2][MAX_VARCHAR_LEN + 24];
        for (int i = 0; i < out_count; i++) vals[i] = valbufs[i];
        vals[out_count] = NULL;

        pthread_rwlock_rdlock(&t1->rw_lock);
        pthread_rwlock_rdlock(&t2->rw_lock);

        int aborted = 0;
        for (int64_t r1 = 0; r1 < t1->row_count && !aborted; r1++) {
            int alive1 = 0;
            read_row(&db->cache, t1->fd, &t1->schema, r1, row1, &alive1);
            if (!alive1) continue;

            int64_t jval1 = (t1->schema.cols[j1].type == COL_VARCHAR)
                ? 0
                : decode_int(row1 + t1->schema.cols[j1].offset);

            for (int64_t r2 = 0; r2 < t2->row_count && !aborted; r2++) {
                int alive2 = 0;
                read_row(&db->cache, t2->fd, &t2->schema, r2, row2, &alive2);
                if (!alive2) continue;

                /* join condition */
                int match = 0;
                if (t1->schema.cols[j1].type == COL_VARCHAR) {
                    char v1[MAX_VARCHAR_LEN], v2[MAX_VARCHAR_LEN];
                    decode_value(&t1->schema.cols[j1], row1 + t1->schema.cols[j1].offset, v1, sizeof(v1));
                    decode_value(&t2->schema.cols[j2], row2 + t2->schema.cols[j2].offset, v2, sizeof(v2));
                    match = (strcmp(v1, v2) == 0);
                } else {
                    int64_t jval2 = decode_int(row2 + t2->schema.cols[j2].offset);
                    match = (jval1 == jval2);
                }
                if (!match) continue;

                /* where clause */
                if (wc.has_where) {
                    int wi1 = find_col(&t1->schema, wc.col);
                    int wi2 = find_col(&t2->schema, wc.col);
                    if (wi1 >= 0) {
                        if (!compare_value(&t1->schema.cols[wi1], row1, wc.op, wc.val)) continue;
                    } else if (wi2 >= 0) {
                        if (!compare_value(&t2->schema.cols[wi2], row2, wc.op, wc.val)) continue;
                    }
                }

                /* emit row */
                for (int i = 0; i < out_count; i++) {
                    int idx2 = out_idx[i];
                    if (idx2 < t1->schema.col_count) {
                        decode_value(&t1->schema.cols[idx2], row1 + t1->schema.cols[idx2].offset, vals[i], MAX_VARCHAR_LEN + 20);
                    } else {
                        int idx3 = idx2 - t1->schema.col_count;
                        decode_value(&t2->schema.cols[idx3], row2 + t2->schema.cols[idx3].offset, vals[i], MAX_VARCHAR_LEN + 20);
                    }
                }
                if (callback) {
                    if (callback(arg, out_count, vals, out_col_names) != 0) aborted = 1;
                }
            }
        }

        pthread_rwlock_unlock(&t2->rw_lock);
        pthread_rwlock_unlock(&t1->rw_lock);
        free(row1); free(row2); free(vals); free(col_names); free(out_col_names);
        return FLEXQL_OK;
    }

    /* ── single-table SELECT ── */
    pthread_rwlock_rdlock(&db->catalog_lock);
    Table *t = find_table(db, tname);
    pthread_rwlock_unlock(&db->catalog_lock);
    if (!t) { *errmsg = make_err("Table %s not found", tname); return FLEXQL_ERROR; }

    /* parse optional WHERE */
    WhereClause wc; memset(&wc, 0, sizeof(wc));
    if (lex_peek(l, peek, sizeof(peek)) && strcasecmp(peek, "WHERE") == 0) {
        lex_next(l, tok, sizeof(tok));
        char wcol[MAX_COL_NAME + MAX_TABLE_NAME + 4];
        lex_next(l, wcol, sizeof(wcol));
        char dot2[4];
        if (lex_peek(l, dot2, sizeof(dot2)) && dot2[0] == '.') {
            lex_next(l, dot2, sizeof(dot2));
            char col2[MAX_COL_NAME];
            lex_next(l, col2, sizeof(col2));
            snprintf(wcol, sizeof(wcol), "%s.%s", wcol, col2);
        }
        char wop[8], wval[MAX_VARCHAR_LEN+16];
        lex_next(l, wop, sizeof(wop));
        lex_next(l, wval, sizeof(wval));
        strncpy(wc.col, wcol, MAX_COL_NAME - 1);
        wc.op = parse_op(wop);
        strncpy(wc.val, wval, sizeof(wc.val) - 1);
        wc.has_where = 1;
    }

    /* resolve output columns */
    int out_idx[MAX_COLS];
    int out_count = 0;
    if (select_star) {
        for (int i = 0; i < t->schema.col_count; i++) out_idx[out_count++] = i;
    } else {
        for (int s2 = 0; s2 < sel_count; s2++) {
            int ci = find_col(&t->schema, sel_cols[s2]);
            if (ci < 0) {
                *errmsg = make_err("Column %s not found", sel_cols[s2]);
                return FLEXQL_ERROR;
            }
            out_idx[out_count++] = ci;
        }
    }

    /* resolve WHERE column */
    int wci = -1;
    if (wc.has_where) {
        wci = find_col(&t->schema, wc.col);
        if (wci < 0) {
            *errmsg = make_err("WHERE column %s not found", wc.col);
            return FLEXQL_ERROR;
        }
    }

    /* build output col_names array */
    char **out_col_names = malloc((size_t)(out_count + 1) * sizeof(char*));
    for (int i = 0; i < out_count; i++) out_col_names[i] = t->schema.cols[out_idx[i]].name;
    out_col_names[out_count] = NULL;

    char *row_buf = malloc((size_t)t->schema.row_size);
    char **vals = malloc((size_t)(out_count + 1) * sizeof(char*));
    char valbufs[MAX_COLS][MAX_VARCHAR_LEN + 24];
    for (int i = 0; i < out_count; i++) vals[i] = valbufs[i];
    vals[out_count] = NULL;

    pthread_rwlock_rdlock(&t->rw_lock);

    /* index shortcut for equality on first column */
    int use_index = 0;
    int64_t idx_row = -1;
    if (wc.has_where && wci == 0 && wc.op == OP_EQ &&
        (t->schema.cols[0].type == COL_INT || t->schema.cols[0].type == COL_DECIMAL)) {
        int64_t key = (int64_t)strtoll(wc.val, NULL, 10);
        idx_row = index_lookup(&t->index, key);
        use_index = 1;
    }

    int aborted = 0;
    int64_t start_r = 0, end_r = t->row_count;
    if (use_index && idx_row >= 0) { start_r = idx_row; end_r = idx_row + 1; }
    else if (use_index)            { start_r = end_r = 0; /* no match */ }

    for (int64_t r = start_r; r < end_r && !aborted; r++) {
        int alive = 0;
        read_row(&db->cache, t->fd, &t->schema, r, row_buf, &alive);
        if (!alive) continue;
        if (wc.has_where && !compare_value(&t->schema.cols[wci], row_buf, wc.op, wc.val)) continue;
        for (int i = 0; i < out_count; i++)
            decode_value(&t->schema.cols[out_idx[i]], row_buf + t->schema.cols[out_idx[i]].offset,
                         vals[i], MAX_VARCHAR_LEN + 20);
        if (callback) {
            if (callback(arg, out_count, vals, out_col_names) != 0) aborted = 1;
        }
    }

    pthread_rwlock_unlock(&t->rw_lock);
    free(row_buf); free(vals); free(out_col_names);
    return FLEXQL_OK;
}

/* ══════════════════════════════════════════════════════════════════
   db_exec – entry point
   ══════════════════════════════════════════════════════════════════ */

int db_exec(Database *db, const char *sql,
            int (*callback)(void*, int, char**, char**),
            void *arg, char **errmsg)
{
    if (errmsg) *errmsg = NULL;
    if (!db || !sql) {
        if (errmsg) *errmsg = make_err("invalid argument");
        return FLEXQL_ERROR;
    }

    char *sql_copy = str_dup(sql);
    str_trim(sql_copy);
    /* strip trailing ; */
    int n = (int)strlen(sql_copy);
    while (n > 0 && (sql_copy[n-1] == ';' || isspace((unsigned char)sql_copy[n-1])))
        sql_copy[--n] = '\0';

    Lexer l;
    l.sql = sql_copy;
    l.pos = 0;

    char cmd[64];
    if (!lex_next(&l, cmd, sizeof(cmd))) {
        free(sql_copy);
        if (errmsg) *errmsg = make_err("Empty statement");
        return FLEXQL_ERROR;
    }
    str_upper(cmd);

    int rc;
    if (strcmp(cmd, "CREATE") == 0) {
        rc = exec_create_table(db, &l, errmsg);
    } else if (strcmp(cmd, "DROP") == 0) {
        rc = exec_drop_table(db, &l, errmsg);
    } else if (strcmp(cmd, "INSERT") == 0) {
        rc = exec_insert(db, &l, errmsg);
    } else if (strcmp(cmd, "DELETE") == 0) {
        rc = exec_delete(db, &l, errmsg);
    } else if (strcmp(cmd, "SELECT") == 0) {
        rc = exec_select(db, &l, callback, arg, errmsg);
    } else {
        if (errmsg) *errmsg = make_err("Unknown command: %s", cmd);
        rc = FLEXQL_ERROR;
    }

    free(sql_copy);
    return rc;
}
