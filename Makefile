# ─── FlexQL Makefile ──────────────────────────────────────────────
#
# Targets:
#   make          – build everything (server + repl + benchmark)
#   make server   – build only the server
#   make repl     – build only the REPL client
#   make bench    – build only the benchmark binary
#   make clean    – remove built artefacts
#   make run-server  – start the server (background)
#   make run-unit    – run unit tests against a running server
#   make run-bench   – run full benchmark (10M rows)

CC      := gcc
CFLAGS  := -std=c11 -O2 -Wall -Wextra -pthread -I./include
LDFLAGS := -pthread -lm

# Source groups
SERVER_SRCS := server/server.c server/storage.c
CLIENT_SRCS := client/flexql_client.c
REPL_SRCS   := repl/repl.c
BENCH_SRCS  := benchmark_flexql.cpp

# Outputs
SERVER_BIN  := flexql_server
REPL_BIN    := flexql_repl
BENCH_BIN   := flexql_bench

.PHONY: all server repl bench clean run-server run-unit run-bench

all: server repl bench

server: $(SERVER_BIN)

$(SERVER_BIN): $(SERVER_SRCS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

repl: $(REPL_BIN)

$(REPL_BIN): $(REPL_SRCS) $(CLIENT_SRCS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

bench: $(BENCH_BIN)

$(BENCH_BIN): $(BENCH_SRCS) $(CLIENT_SRCS)
	g++ -std=c++17 -O2 -Wall -I./include -o $@ $^ $(LDFLAGS)

clean:
	rm -f $(SERVER_BIN) $(REPL_BIN) $(BENCH_BIN)
	rm -rf flexql_data/

run-server: server
	./$(SERVER_BIN) --port 9000 --data-dir ./flexql_data

run-unit: bench
	./$(BENCH_BIN) --unit-test

run-bench: bench
	./$(BENCH_BIN) 10000000
