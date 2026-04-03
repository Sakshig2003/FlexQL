/*
 * repl.c – FlexQL interactive terminal (REPL)
 *
 * Uses the FlexQL client C API (flexql.h) to connect to the server
 * and execute SQL queries typed by the user.
 */

#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../include/flexql.h"

#define DEFAULT_HOST "127.0.0.1"
#define DEFAULT_PORT 9000

static int print_row(void *data, int argc, char **argv, char **azColName)
{
    (void)data;
    for (int i = 0; i < argc; i++) {
        if (i) printf(" | ");
        printf("%s: %s", azColName[i], argv[i] ? argv[i] : "NULL");
    }
    printf("\n");
    return 0;
}

int main(int argc, char **argv)
{
    const char *host = DEFAULT_HOST;
    int         port = DEFAULT_PORT;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--host") == 0 && i + 1 < argc) host = argv[++i];
        else if (strcmp(argv[i], "--port") == 0 && i + 1 < argc) port = atoi(argv[++i]);
        else if (strcmp(argv[i], "--help") == 0) {
            printf("Usage: flexql_repl [--host HOST] [--port PORT]\n");
            return 0;
        }
    }

    FlexQL *db = NULL;
    if (flexql_open(host, port, &db) != FLEXQL_OK) {
        fprintf(stderr, "Cannot connect to %s:%d\n", host, port);
        return 1;
    }
    printf("Connected to FlexQL at %s:%d\n", host, port);
    printf("Type SQL statements followed by ';'. Type 'exit' or Ctrl-D to quit.\n\n");

    char line[4096];
    char query[1024 * 64];
    query[0] = '\0';

    while (1) {
        if (query[0] == '\0') printf("flexql> ");
        else                   printf("     -> ");
        fflush(stdout);

        if (!fgets(line, sizeof(line), stdin)) break;

        /* strip newline */
        int n = (int)strlen(line);
        if (n > 0 && line[n-1] == '\n') line[--n] = '\0';

        if (strcasecmp(line, "exit") == 0 || strcasecmp(line, "quit") == 0) break;
        if (strcmp(line, "") == 0) continue;

        /* accumulate multi-line */
        if (strlen(query) + strlen(line) + 2 < sizeof(query)) {
            if (query[0]) strcat(query, " ");
            strcat(query, line);
        }

        /* execute if we see a ; */
        if (strchr(query, ';')) {
            char *errmsg = NULL;
            int rc = flexql_exec(db, query, print_row, NULL, &errmsg);
            if (rc != FLEXQL_OK) {
                fprintf(stderr, "Error: %s\n", errmsg ? errmsg : "unknown");
                flexql_free(errmsg);
            } else {
                printf("OK\n");
            }
            query[0] = '\0';
        }
    }

    flexql_close(db);
    printf("Bye.\n");
    return 0;
}
