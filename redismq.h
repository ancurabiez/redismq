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

typedef struct rmqContext {
  redisContext *rctx;
  char *host;
  int port;
  int flag;
} rmqContext;

rmqContext* rmq_connect(const char *path_host, int port,
                               int timeout, char **err, int flag);
char* rmq_get(rmqContext **rctx, const char *key, int timeout);
int rmq_put(rmqContext **rctx, const char *key, char *val, int val_len);

int rmq_setter(rmqContext **rctx, const char *key, int expired,
               char *val, int val_len);
int rmq_del(rmqContext **rctx, const char *key);
char* rmq_getter(rmqContext **rctx, const char *key);
void rmq_free(rmqContext *rctx);

#endif
