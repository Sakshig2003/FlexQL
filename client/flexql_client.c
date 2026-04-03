/*
 * flexql_client.c – FlexQL client library
 *
 * Implements flexql_open / flexql_close / flexql_exec / flexql_free.
 * Connects to the server over TCP, sends SQL queries, and processes
 * streaming row/error/done responses.
 */

#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

#include "flexql.h"
#include "protocol.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#ifdef _WIN32
  #include <winsock2.h>
  #include <ws2tcpip.h>
  #pragma comment(lib, "ws2_32.lib")
  typedef SOCKET sock_t;
  #define INVALID_SOCK INVALID_SOCKET
  #define close_sock(s) closesocket(s)
#else
  #include <sys/types.h>
  #include <sys/socket.h>
  #include <netinet/in.h>
  #include <netinet/tcp.h>
  #include <arpa/inet.h>
  #include <netdb.h>
  #include <unistd.h>
  typedef int sock_t;
  #define INVALID_SOCK (-1)
  #define close_sock(s) close(s)
#endif

/* ─── Internal handle ──────────────────────────────────────────── */
struct FlexQL {
    sock_t  fd;
    char    host[256];
    int     port;
};

/* ─── Helpers ──────────────────────────────────────────────────── */

static int write_all(sock_t fd, const char *buf, size_t n)
{
    size_t sent = 0;
    while (sent < n) {
        ssize_t w = send(fd, buf + sent, (int)(n - sent), 0);
        if (w <= 0) return -1;
        sent += (size_t)w;
    }
    return 0;
}

static int read_all(sock_t fd, char *buf, size_t n)
{
    size_t got = 0;
    while (got < n) {
        ssize_t r = recv(fd, buf + got, (int)(n - got), 0);
        if (r <= 0) return -1;
        got += (size_t)r;
    }
    return 0;
}

/* network-order 32-bit helpers */
static void put_u32(char *p, uint32_t v)
{
    p[0] = (char)((v >> 24) & 0xFF);
    p[1] = (char)((v >> 16) & 0xFF);
    p[2] = (char)((v >>  8) & 0xFF);
    p[3] = (char)( v        & 0xFF);
}

static uint32_t get_u32(const char *p)
{
    return ((uint32_t)(unsigned char)p[0] << 24)
         | ((uint32_t)(unsigned char)p[1] << 16)
         | ((uint32_t)(unsigned char)p[2] <<  8)
         | ((uint32_t)(unsigned char)p[3]);
}

static char *make_errmsg(const char *s)
{
    if (!s) s = "unknown error";
    char *m = (char *)malloc(strlen(s) + 1);
    if (m) strcpy(m, s);
    return m;
}

/* ─── API ──────────────────────────────────────────────────────── */

int flexql_open(const char *host, int port, FlexQL **db)
{
    if (!host || !db) return FLEXQL_ERROR;

#ifdef _WIN32
    WSADATA wsa;
    WSAStartup(MAKEWORD(2,2), &wsa);
#endif

    struct addrinfo hints, *res = NULL;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    char port_str[16];
    snprintf(port_str, sizeof(port_str), "%d", port);

    if (getaddrinfo(host, port_str, &hints, &res) != 0 || !res)
        return FLEXQL_ERROR;

    sock_t fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (fd == INVALID_SOCK) { freeaddrinfo(res); return FLEXQL_ERROR; }

    /* TCP_NODELAY – reduces latency for small packets */
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (char*)&one, sizeof(one));

    if (connect(fd, res->ai_addr, (socklen_t)res->ai_addrlen) != 0) {
        freeaddrinfo(res);
        close_sock(fd);
        return FLEXQL_ERROR;
    }
    freeaddrinfo(res);

    FlexQL *h = (FlexQL *)calloc(1, sizeof(FlexQL));
    if (!h) { close_sock(fd); return FLEXQL_ERROR; }
    h->fd   = fd;
    h->port = port;
    strncpy(h->host, host, sizeof(h->host) - 1);

    *db = h;
    return FLEXQL_OK;
}

int flexql_close(FlexQL *db)
{
    if (!db) return FLEXQL_ERROR;
    close_sock(db->fd);
    free(db);
    return FLEXQL_OK;
}

int flexql_exec(FlexQL *db, const char *sql,
                flexql_callback callback, void *arg,
                char **errmsg)
{
    if (!db || !sql) {
        if (errmsg) *errmsg = make_errmsg("invalid argument");
        return FLEXQL_ERROR;
    }

    /* ── Send request ── */
    size_t sql_len = strlen(sql);
    if (sql_len > PROTO_MAX_SQL) {
        if (errmsg) *errmsg = make_errmsg("SQL too long");
        return FLEXQL_ERROR;
    }

    char hdr[4];
    put_u32(hdr, (uint32_t)sql_len);

    if (write_all(db->fd, hdr, 4) < 0 ||
        write_all(db->fd, sql, sql_len) < 0) {
        if (errmsg) *errmsg = make_errmsg("network send error");
        return FLEXQL_ERROR;
    }

    /* ── Receive messages until Done or Error ── */
    char    *col_names_buf = NULL;   /* keeps column names across rows */
    char   **col_names     = NULL;
    int      col_count     = 0;
    int      abort_flag    = 0;
    int      result        = FLEXQL_OK;

    for (;;) {
        /* read 1-byte type + 4-byte length */
        char meta[5];
        if (read_all(db->fd, meta, 5) < 0) {
            if (errmsg) *errmsg = make_errmsg("network recv error");
            result = FLEXQL_ERROR;
            break;
        }

        char     msg_type  = meta[0];
        uint32_t pay_len   = get_u32(meta + 1);

        /* read payload */
        char *payload = NULL;
        if (pay_len > 0) {
            payload = (char *)malloc(pay_len + 1);
            if (!payload) {
                if (errmsg) *errmsg = make_errmsg("out of memory");
                result = FLEXQL_ERROR;
                break;
            }
            if (read_all(db->fd, payload, pay_len) < 0) {
                free(payload);
                if (errmsg) *errmsg = make_errmsg("network recv error");
                result = FLEXQL_ERROR;
                break;
            }
            payload[pay_len] = '\0';
        } else {
            payload = (char *)calloc(1, 1);
        }

        if (msg_type == PROTO_MSG_DONE) {
            free(payload);
            break;  /* success */
        }

        if (msg_type == PROTO_MSG_ERROR) {
            if (errmsg) *errmsg = make_errmsg(payload);
            free(payload);
            result = FLEXQL_ERROR;
            break;
        }

        /* PROTO_MSG_ROW:
         * Format: "COL1\tCOL2\t...\nVAL1\tVAL2\t..."
         * First row also carries column names (before the '\n').
         */
        if (msg_type == PROTO_MSG_ROW) {
            /* split on first '\n' to get header line and value line */
            char *newline = strchr(payload, '\n');
            char *header_part = payload;
            char *value_part  = NULL;

            if (newline) {
                *newline  = '\0';
                value_part = newline + 1;
            }

            /* rebuild col_names from header if needed or first row */
            if (!col_names_buf || col_count == 0) {
                free(col_names_buf);
                free(col_names);
                col_names_buf = strdup(header_part);
                /* count columns */
                col_count = 0;
                if (col_names_buf && col_names_buf[0]) {
                    col_count = 1;
                    for (char *p = col_names_buf; *p; p++)
                        if (*p == '\t') col_count++;
                }
                col_names = (char **)malloc((size_t)(col_count + 1) * sizeof(char *));
                if (col_names && col_names_buf) {
                    char *tok = col_names_buf;
                    for (int i = 0; i < col_count; i++) {
                        col_names[i] = tok;
                        char *tab = strchr(tok, '\t');
                        if (tab) { *tab = '\0'; tok = tab + 1; }
                    }
                    col_names[col_count] = NULL;
                }
            }

            if (!abort_flag && callback && value_part) {
                /* parse values */
                char *vbuf = strdup(value_part);
                char **vals = (char **)malloc((size_t)(col_count + 1) * sizeof(char *));
                if (vbuf && vals) {
                    char *tok = vbuf;
                    for (int i = 0; i < col_count; i++) {
                        vals[i] = tok;
                        char *tab = strchr(tok, '\t');
                        if (tab) { *tab = '\0'; tok = tab + 1; }
                    }
                    vals[col_count] = NULL;
                    int cb_ret = callback(arg, col_count, vals, col_names);
                    if (cb_ret != 0) abort_flag = 1;
                }
                free(vbuf);
                free(vals);
            }

            free(payload);
        } else {
            /* unknown message type – skip */
            free(payload);
        }
    }

    free(col_names_buf);
    free(col_names);
    return result;
}

void flexql_free(void *ptr)
{
    free(ptr);
}
