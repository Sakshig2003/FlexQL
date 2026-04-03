#pragma once
#ifndef FLEXQL_PROTOCOL_H
#define FLEXQL_PROTOCOL_H

/*
 * Wire protocol (all integers in network byte order / big-endian)
 *
 * Client → Server:
 *   [4 bytes: payload length][N bytes: SQL string (no NUL)]
 *
 * Server → Client:
 *   [1 byte: msg type][4 bytes: payload length][N bytes: payload]
 *
 * Message types:
 *   'R' – Row    : payload = tab-separated column names\nvalues (repeated)
 *   'E' – Error  : payload = error message string
 *   'D' – Done   : payload = empty (signals end of result set / success)
 */

#define PROTO_MSG_ROW   'R'
#define PROTO_MSG_ERROR 'E'
#define PROTO_MSG_DONE  'D'

#define PROTO_MAX_SQL   (4 * 1024 * 1024)   /* 4 MB max query size */

#endif /* FLEXQL_PROTOCOL_H */
