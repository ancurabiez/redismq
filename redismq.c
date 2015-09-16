/*********************************************/
/*** Copyright (c) 2014, Lulus Wijayakto   ***/
/***                                       ***/
/*** Email : l.wijayakto@yahoo.com         ***/
/***         l.wijayakto@gmail.com         ***/
/***                                       ***/
/*** License: BSD 3-Clause
/*********************************************/

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <assert.h>

#include "redismq.h"



rmqContext* rmq_connect(const char *path_host, int port, int timeout,
                           char **err, int flag)
{
   struct timeval tv;
   rmqContext *rContext;
   int i;

   tv.tv_sec = timeout;
   tv.tv_usec = 0;

   rContext = je_malloc(sizeof(struct rmqContext));
   assert (rContext);

   memset (rContext, 0, sizeof(struct rmqContext));

   rContext->host = je_malloc(16);
   assert (rContext->host);
   memset(rContext->host, 0, 16);

   memcpy(rContext->host, path_host, 15);

   rContext->port = port;
   rContext->flag = flag;

   if (flag == UNIX_SOCKET)
      rContext->rctx = redisConnectUnixWithTimeout(path_host, tv);
   else
      rContext->rctx = redisConnectWithTimeout(path_host, port, tv);

   if (!rContext->rctx || rContext->rctx->err) {
       char b1[128];

      if (rContext->rctx) {
         snprintf(b1, 128, "Queue connection error: %s\n", rContext->rctx->errstr);
         redisFree(rContext->rctx);

         i = strlen(b1);

         *err = je_malloc(i + 1);
         assert(*err);
         memset(*err, 0, i);
         memcpy(*err, b1, i);
         
         return NULL;
      } else {
         snprintf(b1, 128, "Queue connection error: can't "
                           "allocate redis context\n");
         i = strlen(b1);

         *err = je_malloc(i + 1);
         assert(*err);
         memset(*err, 0, i);
         memcpy(*err, b1, i);
         
         return NULL;
      }
   }

   return rContext;
}

void rmq_free(rmqContext *rctx)
{
  redisFree(rctx->rctx);
  je_free(rctx->host);
  je_free(rctx);
}

/* reconnect */
static void rmq_reconnect(rmqContext **rctx)
{
   rmqContext *rctx__ = NULL;
   char *err;
   char host__[16];
   int i = 0;
   int port__;
   int flag__;

   memset (host__, 0, 16);

   memcpy(host__, (*rctx)->host, strlen((*rctx)->host));
   port__ = (*rctx)->port;
   flag__ = (*rctx)->flag;

   rmq_free(*rctx);

   // trying reconnect till 60 times
   while (1) {
      rctx__ = rmq_connect(host__, port__, 1, &err, flag__);
      if (!rctx__) {
         i++;

         if (i >= 60) //give up
            exit(1);

         je_free(err);
         sleep(1);
      }
      else
         break;
   }

   *rctx = rctx__;
}

/* return : NULL if error */
char* rmq_get(rmqContext **rctx, const char *key, int timeout)
{
   redisReply *rep = NULL;
   char *res = NULL;

   if (timeout > 0)
      rep = redisCommand((*rctx)->rctx, "BLPOP redismq:%s %d", key, timeout);
   else
      rep = redisCommand((*rctx)->rctx, "LPOP redismq:%s", key);

   if (!rep) {
     rmq_reconnect(rctx);

     // re-execute
      if (timeout > 0)
         rep = redisCommand((*rctx)->rctx, "BLPOP redismq:%s %d", key, timeout);
      else
         rep = redisCommand((*rctx)->rctx, "LPOP redismq:%s", key);
   }

   if ((rep->type == REDIS_REPLY_STRING) && (rep->elements == 0)) {
      if (rep->str) {
         res = je_malloc(rep->len);
         assert(res);

         memcpy(res, rep->str, rep->len);
      }
   }
   else if ((rep->type == REDIS_REPLY_ARRAY) && (rep->elements == 2)) {
      if (rep->element[1]->str) {
         res = je_malloc(rep->element[1]->len);
         assert (res);

         memcpy(res, rep->element[1]->str, rep->element[1]->len);
      }
   }

   freeReplyObject(rep);
   return res;
}

/* return : 1 on success */
/*          0 on error */
int rmq_put(rmqContext **rctx, const char *key, char *val, int val_len)
{
   redisReply *rep = NULL;
   int ret = 0;
   
   rep = redisCommand((*rctx)->rctx, "RPUSH redismq:%s %b", key, val, val_len);
   if (!rep) {
      rmq_reconnect(rctx);

      //re-execute
      rep = redisCommand((*rctx)->rctx, "RPUSH redismq:%s %b", key, val, val_len);
   }

   if (rep->type == REDIS_REPLY_INTEGER)
     ret = 1;

   freeReplyObject(rep);
   return ret;
}

int rmq_setter(rmqContext **rctx, const char *key, int expired,
               char *val, int val_len)
{
   redisReply *rep = NULL;
   int ret = 0;

   rep = redisCommand((*rctx)->rctx, "SETEX %s %d %b", key, expired,
                      val, val_len);

   if (!strcmp(rep->str, "OK"))
      ret = 1;

   freeReplyObject(rep);
   return ret;
}

int rmq_del(rmqContext **rctx, const char *key)
{
   redisReply *rep = NULL;
   int ret = 0;

   rep = redisCommand((*rctx)->rctx, "DEL %s", key);

   if (rep->integer > 0)
      ret = 1;

   freeReplyObject(rep);
   return ret;
}

char* rmq_getter(rmqContext **rctx, const char *key)
{
   redisReply *rep = NULL;
   char *res = NULL;

   rep = redisCommand((*rctx)->rctx, "GET %s", key);

   if (rep->len > 0) {
     if (rep->str) {
        res = je_malloc(rep->len);
        assert(res);

        memcpy(res, rep->str, rep->len);
     }
   }

   freeReplyObject(rep);
   return res;
}
