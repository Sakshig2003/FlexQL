#pragma once
#ifndef FLEXQL_H
#define FLEXQL_H

#ifdef __cplusplus
extern "C" {
#endif

/* ─── Error codes ─────────────────────────────────────────────── */
#define FLEXQL_OK    0
#define FLEXQL_ERROR 1

/* ─── Opaque database handle ──────────────────────────────────── */
typedef struct FlexQL FlexQL;

/* ─── Callback type for SELECT results ───────────────────────── */
/*
 * Invoked once per result row.
 * data       – user pointer passed through flexql_exec's `arg`
 * argc       – number of columns
 * argv       – column values (NULL-terminated C strings, or NULL)
 * azColName  – column names
 * Return 0 to continue, non-zero to abort.
 */
typedef int (*flexql_callback)(void *data, int argc,
                               char **argv, char **azColName);

/* ─── Public API ──────────────────────────────────────────────── */

/**
 * Open a connection to the FlexQL server.
 *
 * @param host   IP address or hostname of the server
 * @param port   TCP port the server is listening on
 * @param db     OUT – receives the allocated handle
 * @return FLEXQL_OK on success, FLEXQL_ERROR on failure
 */
int flexql_open(const char *host, int port, FlexQL **db);

/**
 * Close the connection and free all resources.
 *
 * @param db  Handle returned by flexql_open
 * @return FLEXQL_OK on success, FLEXQL_ERROR on failure
 */
int flexql_close(FlexQL *db);

/**
 * Execute an SQL statement.
 *
 * For SELECT queries the callback (if non-NULL) is called once per row.
 * On error *errmsg is set to a heap-allocated message; the caller must
 * free it with flexql_free().
 *
 * @param db       Open database handle
 * @param sql      NULL-terminated SQL string
 * @param callback Row callback (may be NULL)
 * @param arg      User pointer forwarded to callback
 * @param errmsg   OUT – receives error description on failure (may be NULL)
 * @return FLEXQL_OK on success, FLEXQL_ERROR on failure
 */
int flexql_exec(FlexQL *db, const char *sql,
                flexql_callback callback, void *arg,
                char **errmsg);

/**
 * Free a string allocated by the FlexQL library (e.g. error messages).
 *
 * @param ptr  Pointer returned by the library (no-op if NULL)
 */
void flexql_free(void *ptr);

#ifdef __cplusplus
}
#endif

#endif /* FLEXQL_H */
