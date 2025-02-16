#include "api.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "api.h"
#include "src/common/constants.h"
#include "src/common/io.h"
#include "src/common/protocol.h"

/*----------------------------GLOBAL VARIABLES-------------------------------*/

//So the client doesn't close the files multiple times
int already_closed = 0;

int req_pipe_fd;
int resp_pipe_fd;
int notif_pipe_fd;

char req_path[MAX_PIPE_PATH_LENGTH];
char resp_path[MAX_PIPE_PATH_LENGTH];
char notif_path[MAX_PIPE_PATH_LENGTH];

int notifs = 1;
int current_subs = 0;
pthread_rwlock_t subs_rwlock = PTHREAD_RWLOCK_INITIALIZER;
pthread_mutex_t stdout_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t notifs_mutex = PTHREAD_MUTEX_INITIALIZER;

/*----------------------------CLEANUP FUNCTIONS------------------------------*/

// Unlink all client side fifo pipes
void unlink_client_pipes() {
  safe_unlink(req_path);
  safe_unlink(resp_path);
  safe_unlink(notif_path);
}
// Close all client side fifo pipes
void close_client_pipes() {
  pthread_mutex_lock(&stdout_mutex);
  if(!already_closed){
    safe_close(req_pipe_fd);
    safe_close(resp_pipe_fd);
    safe_close(notif_pipe_fd);
    already_closed = 1;
  }
  pthread_mutex_unlock(&stdout_mutex);
}

/*------------------------------KVS FUNCTIONS--------------------------------*/

int kvs_connect(char const *req_pipe_path, char const *resp_pipe_path,
                char const *server_pipe_path, char const *notif_pipe_path) {

  // store the paths
  strncpy(req_path, req_pipe_path, MAX_PIPE_PATH_LENGTH);
  strncpy(resp_path, resp_pipe_path, MAX_PIPE_PATH_LENGTH);
  strncpy(notif_path, notif_pipe_path, MAX_PIPE_PATH_LENGTH);
  // create pipes
  if (safe_mkfifo(req_pipe_path, 0666)) { // requests pipe
    return 1;
  }

  if (safe_mkfifo(resp_pipe_path, 0666)) { // responses pipe
    safe_unlink(req_pipe_path);
    return 1;
  }
  if (safe_mkfifo(notif_pipe_path, 0666)) { // notifications pipe
    safe_unlink(req_pipe_path);
    safe_unlink(resp_pipe_path);
    return 1;
  }
  // open pipes
  int server_id = safe_open(server_pipe_path, O_WRONLY);
  if (server_id == -1) {
    unlink_client_pipes();
    return 1;
  }
  char msg[1 + 3 * MAX_PIPE_PATH_LENGTH] = {0};
  msg[0] = OP_CODE_CONNECT;
  strncpy(msg + 1, req_pipe_path, MAX_PIPE_PATH_LENGTH);
  strncpy(msg + 1 + MAX_PIPE_PATH_LENGTH, resp_pipe_path, MAX_PIPE_PATH_LENGTH);
  strncpy(msg + 1 + 2 * MAX_PIPE_PATH_LENGTH, notif_pipe_path,
          MAX_PIPE_PATH_LENGTH);
  if (safe_write(server_id, msg, sizeof(msg))) {
    close_client_pipes();
    unlink_client_pipes();
    return 1;
  }
  req_pipe_fd = safe_open(req_pipe_path, O_WRONLY);
  if (req_pipe_fd == -1) {
    unlink_client_pipes();
    return 1;
  }
  resp_pipe_fd = safe_open(resp_pipe_path, O_RDONLY);
  if (resp_pipe_fd == -1) {
    unlink_client_pipes();
    safe_close(req_pipe_fd);
    return 1;
  }
  notif_pipe_fd = safe_open(notif_pipe_path, O_RDONLY);
  if (notif_pipe_fd == -1) {
    unlink_client_pipes();
    safe_close(req_pipe_fd);
    safe_close(resp_pipe_fd);
    return 1;
  }
  char resp[2];
  if (read_all(resp_pipe_fd, resp, sizeof(resp), 0) <= 0) {
    close_client_pipes();
    unlink_client_pipes();
    return 3;
  }
  pthread_mutex_lock(&stdout_mutex);
  printf("Server returned %d for operation: connect\n", resp[1]);
  pthread_mutex_unlock(&stdout_mutex);
  if (resp[1] != 0) {
    close_client_pipes();
    unlink_client_pipes();
    return 1;
  }
  safe_close(server_id);
  return 0;
}

int kvs_disconnect(void) {
  char request[1];
  request[0] = OP_CODE_DISCONNECT;
  int write_result = safe_write(req_pipe_fd, request, sizeof(request));
  if (write_result == -1) {
    fprintf(stderr, "Error sending disconnect request\n");
    return -1;
  } else if (write_result == 1 || write_result == 2) {
    close_client_pipes();
    unlink_client_pipes();
    pthread_mutex_lock(&notifs_mutex);
    notifs = 0;
    pthread_mutex_unlock(&notifs_mutex);
    return 3;
  }

  char response[2];
  if (read_all(resp_pipe_fd, response, 2, 0) <= 0) {
    pthread_mutex_lock(&stdout_mutex);
    fprintf(stderr, "Error reading from request pipe\n");
    pthread_mutex_unlock(&stdout_mutex);
    close_client_pipes();
    unlink_client_pipes();
    return 3;
  }
  pthread_mutex_lock(&stdout_mutex);
  printf("Server returned %d for operation: disconnect\n", response[1]);
  pthread_mutex_unlock(&stdout_mutex);
  if (response[1] != 0) {
    pthread_mutex_lock(&stdout_mutex);
    fprintf(stderr, "Server failed to disconnect\n");
    pthread_mutex_unlock(&stdout_mutex);
    return 1;
  }
  close_client_pipes();
  unlink_client_pipes();
  return 0;
}

int kvs_subscribe(const char *key) {
  // send subscribe message to request pipe and wait for response in response
  // pipe
  pthread_rwlock_rdlock(&subs_rwlock);
  if (current_subs >= MAX_NUMBER_SUB) {
    fprintf(stderr, "Max number of subscriptions reached\n");
    pthread_rwlock_unlock(&subs_rwlock);
    return 1;
  }
  pthread_rwlock_unlock(&subs_rwlock);

  char request[1 + MAX_STRING_SIZE] = {0};
  request[0] = OP_CODE_SUB;
  strncpy(request + 1, key, MAX_STRING_SIZE);

  int write_result = safe_write(req_pipe_fd, request, sizeof(request));
  if (write_result == -1) {
    fprintf(stderr, "Error sending subscribe request\n");
    return -1;
  } else if (write_result == 1 || write_result == 2) {
    close_client_pipes();
    unlink_client_pipes();
    pthread_mutex_lock(&notifs_mutex);
    notifs = 0;
    pthread_mutex_unlock(&notifs_mutex);
    return 3;
  }

  char response[2];
  if (read_all(resp_pipe_fd, response, 2, NULL) <= 0) {
    fprintf(stderr, "Error reading from request pipe\n");
    close_client_pipes();
    unlink_client_pipes();
    return 3;
  }
  pthread_mutex_lock(&stdout_mutex);
  printf("Server returned %d for operation: subscribe\n", response[1]);
  pthread_mutex_unlock(&stdout_mutex);
  if (response[1]) {
    pthread_rwlock_wrlock(&subs_rwlock);
    current_subs++;
    pthread_rwlock_unlock(&subs_rwlock);
  }
  return 0;
}

int kvs_unsubscribe(const char *key) {
  // send unsubscribe message to request pipe and wait for response in response
  char request[1 + MAX_STRING_SIZE] = {0};
  request[0] = OP_CODE_UNSUB;
  strncpy(request + 1, key, MAX_STRING_SIZE);

  int write_result = safe_write(req_pipe_fd, request, sizeof(request));
  if (write_result == -1) {
    fprintf(stderr, "Error sending unsubscribe request\n");
    return -1;
  } else if (write_result == 1 || write_result == 2) {
    close_client_pipes();
    unlink_client_pipes();
    pthread_mutex_lock(&notifs_mutex);
    notifs = 0;
    pthread_mutex_unlock(&notifs_mutex);
    return 3;
  }
  char response[2];
  if (read_all(resp_pipe_fd, response, 2, 0) <= 0) {
    close_client_pipes();
    unlink_client_pipes();
    return 3;
  }
  pthread_mutex_lock(&stdout_mutex);
  printf("Server returned %d for operation: unsubscribe\n", response[1]);
  pthread_mutex_unlock(&stdout_mutex);
  if (response[1] != 0) {
    return 1;
  }
  if (response[1] == 0) {
    pthread_rwlock_wrlock(&subs_rwlock);
    current_subs--;
    pthread_rwlock_unlock(&subs_rwlock);
  }
  return 0;
}

/*--------------------------NOTIFICATIONS THREAD-----------------------------*/

void *notifications_thread() {
  char buffer[MAX_STRING_SIZE * 2 + 1];
  while (1) {
    pthread_mutex_lock(&notifs_mutex);
    if (!notifs) {
      pthread_mutex_unlock(&notifs_mutex);
      break;
    }
    pthread_mutex_unlock(&notifs_mutex);
    if (read_all(notif_pipe_fd, buffer, 1 + MAX_STRING_SIZE * 2, 0) != 1) {
      close_client_pipes();
      unlink_client_pipes();
      exit(1);
    }
    char notif_code = buffer[0];
    char key[MAX_STRING_SIZE + 1] = {0};
    char value[MAX_STRING_SIZE + 1] = {0};
    strncpy(key, buffer + 1, MAX_STRING_SIZE);
    strncpy(value, buffer + 1 + MAX_STRING_SIZE, MAX_STRING_SIZE);
    pthread_mutex_lock(&stdout_mutex);
    fprintf(stdout, "(%s,%s)\n", key, value);
    pthread_mutex_unlock(&stdout_mutex);
    if (notif_code == 2) {
      pthread_rwlock_wrlock(&subs_rwlock);
      current_subs--;
      pthread_rwlock_unlock(&subs_rwlock);
    }
  }
  return NULL;
}