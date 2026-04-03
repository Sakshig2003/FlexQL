/*
 * server.c – FlexQL multithreaded TCP server
 *
 * Architecture:
 *   - Main thread listens on the configured port.
 *   - Each accepted connection is handed to a new pthread (thread-per-connection).
 *   - All database operations go through the shared Database handle which uses
 *     reader/writer locks for safe concurrent access.
 *
 * Wire protocol: see include/protocol.h
 */

#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <signal.h>
#include <unistd.h>
#include <dirent.h>
#include <pthread.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

#include "../include/protocol.h"
#include "../include/flexql.h"
#include "storage.h"

#define DEFAULT_PORT     9000
#define DEFAULT_DATA_DIR "./flexql_data"
#define BACKLOG          128

/* ═════════════════════════════════════════════════════════════════
   Globals
   ═════════════════════════════════════════════════════════════════ */

static Database *g_db   = NULL;
static int       g_server_fd = -1;
static volatile int g_running = 1;

/* ═════════════════════════════════════════════════════════════════
   Wire helpers
   ═════════════════════════════════════════════════════════════════ */

static int write_all(int fd, const char *buf, size_t n)
{
    size_t sent = 0;
    while (sent < n) {
        ssize_t w = write(fd, buf + sent, n - sent);
        if (w <= 0) return -1;
        sent += (size_t)w;
    }
    return 0;
}

static int read_all(int fd, char *buf, size_t n)
{
    size_t got = 0;
    while (got < n) {
        ssize_t r = read(fd, buf + got, n - got);
        if (r <= 0) return -1;
        got += (size_t)r;
    }
    return 0;
}

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

/* Send a single protocol message to the client */
static int send_msg(int fd, char type, const char *payload, uint32_t len)
{
    char meta[5];
    meta[0] = type;
    put_u32(meta + 1, len);
    if (write_all(fd, meta, 5) < 0) return -1;
    if (len > 0 && write_all(fd, payload, len) < 0) return -1;
    return 0;
}

static int send_done(int fd)  { return send_msg(fd, PROTO_MSG_DONE, NULL, 0); }
static int send_error(int fd, const char *msg)
{
    return send_msg(fd, PROTO_MSG_ERROR, msg, (uint32_t)strlen(msg));
}

/* ═════════════════════════════════════════════════════════════════
   Callback: stream each SELECT row back to the client
   ═════════════════════════════════════════════════════════════════ */

typedef struct {
    int    client_fd;
    int    aborted;
    int    first_row;       /* used to decide whether to re-send header */
    char   col_header[4096];
    int    col_header_len;
} RowSender;

static int row_sender_callback(void *data, int argc, char **argv, char **azColName)
{
    RowSender *rs = (RowSender *)data;
    if (rs->aborted) return 1;

    /* Build: "COL1\tCOL2\t...\nVAL1\tVAL2\t..." */
    char buf[65536];
    int  pos = 0;

    /* header */
    if (rs->first_row) {
        for (int i = 0; i < argc; i++) {
            if (i) buf[pos++] = '\t';
            const char *cn = azColName[i] ? azColName[i] : "";
            int cl = (int)strlen(cn);
            if (pos + cl >= (int)sizeof(buf) - 4) break;
            memcpy(buf + pos, cn, (size_t)cl);
            pos += cl;
        }
        rs->first_row = 0;
    } else {
        /* re-use saved header */
        if (rs->col_header_len > 0 && rs->col_header_len < (int)sizeof(buf) - 4) {
            memcpy(buf, rs->col_header, (size_t)rs->col_header_len);
            pos = rs->col_header_len;
        }
    }

    /* save header for subsequent rows */
    if (rs->col_header_len == 0) {
        memcpy(rs->col_header, buf, (size_t)pos);
        rs->col_header_len = pos;
    }

    buf[pos++] = '\n';

    /* values */
    for (int i = 0; i < argc; i++) {
        if (i) buf[pos++] = '\t';
        const char *v = argv[i] ? argv[i] : "NULL";
        int vl = (int)strlen(v);
        if (pos + vl >= (int)sizeof(buf) - 4) break;
        memcpy(buf + pos, v, (size_t)vl);
        pos += vl;
    }

    if (send_msg(rs->client_fd, PROTO_MSG_ROW, buf, (uint32_t)pos) < 0) {
        rs->aborted = 1;
        return 1;
    }
    return 0;
}

/* ═════════════════════════════════════════════════════════════════
   Client handler thread
   ═════════════════════════════════════════════════════════════════ */

static void *client_thread(void *arg)
{
    int cfd = *(int*)arg;
    free(arg);

    pthread_detach(pthread_self());

    for (;;) {
        /* read 4-byte length */
        char len_buf[4];
        if (read_all(cfd, len_buf, 4) < 0) break;
        uint32_t sql_len = get_u32(len_buf);
        if (sql_len == 0 || sql_len > PROTO_MAX_SQL) {
            send_error(cfd, "SQL length out of range");
            break;
        }

        char *sql = malloc(sql_len + 1);
        if (!sql) { send_error(cfd, "OOM"); break; }
        if (read_all(cfd, sql, sql_len) < 0) { free(sql); break; }
        sql[sql_len] = '\0';

        RowSender rs;
        memset(&rs, 0, sizeof(rs));
        rs.client_fd  = cfd;
        rs.first_row  = 1;

        char *errmsg = NULL;
        int rc = db_exec(g_db, sql, row_sender_callback, &rs, &errmsg);
        free(sql);

        if (rc != FLEXQL_OK) {
            const char *em = errmsg ? errmsg : "execution error";
            send_error(cfd, em);
            free(errmsg);
        } else {
            send_done(cfd);
        }
    }

    close(cfd);
    return NULL;
}

/* ═════════════════════════════════════════════════════════════════
   Signal handler
   ═════════════════════════════════════════════════════════════════ */

static void sig_handler(int sig)
{
    (void)sig;
    g_running = 0;
    if (g_server_fd >= 0) close(g_server_fd);
}

/* ═════════════════════════════════════════════════════════════════
   main
   ═════════════════════════════════════════════════════════════════ */

int main(int argc, char **argv)
{
    int port         = DEFAULT_PORT;
    const char *ddir = DEFAULT_DATA_DIR;

    int reset_data = 0;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            port = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--data-dir") == 0 && i + 1 < argc) {
            ddir = argv[++i];
        } else if (strcmp(argv[i], "--reset") == 0) {
            reset_data = 1;
        } else if (strcmp(argv[i], "--help") == 0) {
            printf("Usage: flexql_server [--port PORT] [--data-dir DIR] [--reset]\n");
            printf("  --port      TCP port to listen on (default: %d)\n", DEFAULT_PORT);
            printf("  --data-dir  Directory for persistent data (default: %s)\n", DEFAULT_DATA_DIR);
            printf("  --reset     Wipe all data on startup (fresh start)\n");
            return 0;
        }
    }

    /* --reset: delete all .tbl files in data dir */
    if (reset_data) {
        DIR *rdir = opendir(ddir);
        if (rdir) {
            struct dirent *rde;
            while ((rde = readdir(rdir)) != NULL) {
                char *dot = strrchr(rde->d_name, '.');
                if (dot && strcmp(dot, ".tbl") == 0) {
                    char rpath[600];
                    snprintf(rpath, sizeof(rpath), "%s/%s", ddir, rde->d_name);
                    unlink(rpath);
                }
            }
            closedir(rdir);
        }
        printf("Data directory reset.\n");
    }

    /* ignore SIGPIPE (client disconnects) */
    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT,  sig_handler);
    signal(SIGTERM, sig_handler);

    /* open database */
    if (db_open(ddir, &g_db) != FLEXQL_OK) {
        fprintf(stderr, "Failed to open database at %s\n", ddir);
        return 1;
    }
    printf("FlexQL server started. Data dir: %s\n", ddir);

    /* create listening socket */
    g_server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (g_server_fd < 0) { perror("socket"); db_close(g_db); return 1; }

    int opt = 1;
    setsockopt(g_server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons((uint16_t)port);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(g_server_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind"); close(g_server_fd); db_close(g_db); return 1;
    }
    if (listen(g_server_fd, BACKLOG) < 0) {
        perror("listen"); close(g_server_fd); db_close(g_db); return 1;
    }

    printf("Listening on 0.0.0.0:%d\n", port);

    while (g_running) {
        struct sockaddr_in cli_addr;
        socklen_t cli_len = sizeof(cli_addr);
        int cfd = accept(g_server_fd, (struct sockaddr*)&cli_addr, &cli_len);
        if (cfd < 0) {
            if (g_running) perror("accept");
            continue;
        }

        /* TCP_NODELAY for low-latency interactive queries */
        int one = 1;
        setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

        int *cfd_ptr = malloc(sizeof(int));
        if (!cfd_ptr) { close(cfd); continue; }
        *cfd_ptr = cfd;

        pthread_t tid;
        if (pthread_create(&tid, NULL, client_thread, cfd_ptr) != 0) {
            perror("pthread_create");
            close(cfd);
            free(cfd_ptr);
        }
    }

    printf("\nShutting down...\n");
    db_close(g_db);
    return 0;
}
