/********************************************
*** Copyright (c) 2014, Lulus Wijayakto   ***
***                                       ***
*** Email : l.wijayakto@yahoo.com         ***
***         l.wijayakto@gmail.com         ***
***                                       ***
*** License: BSD 3-Clause                 ***
*********************************************/

#ifndef REDIS_MQ_H
#define REDIS_MQ_H

#include <hiredis/hiredis.h>

#define TCP_SOCKET 0
#define UNIX_SOCKET 1


/* using unix socket: port = 0
 *                    flag = UNIX_SOCKET
 *                    path_host = socket path
 * if return null, err must be free
 */
redisContext* rmq_connect(const char *path_host, int port,
                               int timeout, char **err, int flag);

/* return null if error or value not found
 * return must be free
 */
char* rmq_get(redisContext **rctx, const char *key, int timeout);

/* return 0 if error */
int rmq_put(redisContext **rctx, const char *key, char *val, int val_len);

#endif
