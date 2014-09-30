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


char host__[32];
int port__;
int flag__;
int once = 0;


redisContext* rmq_connect(const char *path_host, int port, int timeout,
                           char **err, int flag)
{
   struct timeval tv;
   redisContext *rctx;

   tv.tv_sec = timeout;
   tv.tv_usec = 0;

   if (!once) {
      strncpy(host__, path_host, strlen(path_host));
      port__ = port;
      flag__ = flag;

      once = 1;
   }

   if (flag == UNIX_SOCKET)
      rctx = redisConnectUnixWithTimeout(path_host, tv);
   else
      rctx = redisConnectWithTimeout(path_host, port, tv);

   if (!rctx || rctx->err) {
      if (rctx) {
         char b1[128];

         snprintf(b1, 128, "Queue connection error: %s\n", rctx->errstr);
         redisFree(rctx);
         *err = strdup(b1);
         assert(err);
         
         return NULL;
      } else {
         *err = strdup("Queue connection error: can't "
                        "allocate redis context\n");
         assert(err);
         
         return NULL;
      }
   }

   return rctx;
}

/* reconnect */
static void rmq_reconnect(redisContext **rctx)
{
   redisContext *rctx__;
   char *err;
   int i = 0;

   // trying reconnect till 60 times
   while (1) {
      rctx__ = rmq_connect(host__, port__, 1, &err, flag__);
      if (!rctx__) {
         i++;

         if (i >= 60) //give up
            exit(1);

         free(err);
         sleep(1);
      }
      else
         break;
   }

   redisFree(*rctx);
   *rctx = rctx__;
}

static redisReply* multi_command_put(redisContext **rctx, const char *key,
                                     char *val, int val_len)
{
   redisReply *rep;

   rep = redisCommand(*rctx, "MULTI");
   freeReplyObject(rep);

   // add put statistic (if not exists)
   rep = redisCommand(*rctx, "SETNX redismq:%s:stat:put 0", key);
   freeReplyObject(rep);

   // put data to queue
   rep = redisCommand(*rctx, "LPUSH redismq:%s %b", key, val, val_len);
   freeReplyObject(rep);

   // increment put stat
   rep = redisCommand(*rctx, "INCR redismq:%s:stat:put", key);
   freeReplyObject(rep);

   rep = redisCommand(*rctx, "EXEC");

   return rep;
}

/* return : NULL if error */
char* rmq_get(redisContext **rctx, const char *key, int timeout)
{
   redisReply *rep = NULL;
   char *res = NULL;

   // add get statistic (if not exists)
   rep = redisCommand(*rctx, "SETNX redismq:%s:stat:get 0", key);
   if (!rep) {
      rmq_reconnect(rctx);

      //re-execute
      rep = redisCommand(*rctx, "SETNX redismq:%s:stat:get 0", key);
   }
   freeReplyObject(rep);
   
   if (timeout > 0)
      rep = redisCommand(*rctx, "BRPOP redismq:%s %d", key, timeout);
   else
      rep = redisCommand(*rctx, "RPOP redismq:%s", key);

   if (!rep) {
     rmq_reconnect(rctx);

     // re-execute
      if (timeout > 0)
         rep = redisCommand(*rctx, "BRPOP redismq:%s %d", key, timeout);
      else
         rep = redisCommand(*rctx, "RPOP redismq:%s", key);
   }

   if ((rep->type == 1) && (rep->elements == 0)) {
      if (rep->str) {
         res = malloc(rep->len + 1);
         assert(res);

         memset(res, 0, rep->len +1);
         memcpy(res, rep->str, rep->len);
         freeReplyObject(rep);

         // increment put stat
         rep = redisCommand(*rctx, "INCR redismq:%s:stat:get", key);
         freeReplyObject(rep);
      } else
         freeReplyObject(rep);
   }
   else if ((rep->type == 2) && (rep->elements == 2)) {
      if (rep->element[1]->str) {
         res = malloc(rep->element[1]->len + 1);
         assert (res);

         memset(res, 0, rep->element[1]->len +1);
         memcpy(res, rep->element[1]->str, rep->element[1]->len);
      
         freeReplyObject(rep);

         // increment put stat
         rep = redisCommand(*rctx, "INCR redismq:%s:stat:get", key);
         freeReplyObject(rep);
      } else
         freeReplyObject(rep);
   } else
      freeReplyObject(rep);

   return res;
}

/* return : 1 on success */
/*          0 on error */
int rmq_put(redisContext **rctx, const char *key, char *val, int val_len)
{
   redisReply *rep = NULL;
   int ret = 0;
   
   rep = multi_command_put(rctx, key, val, val_len);
   if (!rep) {
      rmq_reconnect(rctx);

      //re-execute
      rep = multi_command_put(rctx, key, val, val_len);
   }

   if (rep->elements == 4)
      ret = 1;

   freeReplyObject(rep);

   return ret;
}

