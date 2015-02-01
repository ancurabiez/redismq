/*********************************************/
/*** Copyright (c) 2014, Lulus Wijayakto   ***/
/***                                       ***/
/*** Email : l.wijayakto@yahoo.com         ***/
/***         l.wijayakto@gmail.com         ***/
/***                                       ***/
/*** License: BSD 3-Clause                 ***/
/*********************************************/

#ifndef REDIS_MQ_H
#define REDIS_MQ_H

#include <hiredis/hiredis.h>
#include <jemalloc/jemalloc.h>

#define TCP_SOCKET 0
#define UNIX_SOCKET 1

redisContext* rmq_connect(const char *path_host, int port,
                               int timeout, char **err, int flag);
char* rmq_get(redisContext **rctx, const char *key, int timeout);
int rmq_put(redisContext **rctx, const char *key, char *val, int val_len);

#endif
