#define _DEFAULT_SOURCE

#include "constants.h"
#include "operations.h"
#include "parser.h"
#include "src/common/constants.h"
#include "src/common/io.h"
#include "src/common/protocol.h"
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <math.h>
#include <pthread.h>
#include <semaphore.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

/*----------------------------CONSUMER/PRODUCER------------------------------*/
typedef struct {
  char req_pipe_path[MAX_PIPE_PATH_LENGTH];
  char resp_pipe_path[MAX_PIPE_PATH_LENGTH];
  char notif_pipe_path[MAX_PIPE_PATH_LENGTH];
} Client;

Client clients[MAX_SESSION_COUNT];
int in = 0;
int out = 0;

sem_t empty;
sem_t full;
pthread_mutex_t buffer_lock = PTHREAD_MUTEX_INITIALIZER;
/*End of Consumer Producer*/

/*----------------------------GLOBAL VARIABLES-------------------------------*/

DIR *dir;
int MAX_PROC;
int active_child = 0;

typedef struct {
  char *dir_path;
} ThreadArgs;

char pipe_name[MAX_PIPE_PATH_LENGTH];
pthread_mutex_t dir_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t active_child_mutex = PTHREAD_MUTEX_INITIALIZER;
SubscriptionList *subs_list = NULL;
ActiveClientsList *active_clients_list = NULL;

/*------------------------------SIGNAL STUFF---------------------------------*/

volatile sig_atomic_t received_sigusr1 = 0;
pthread_mutex_t sigusr1_mutex = PTHREAD_MUTEX_INITIALIZER;

void sig_handler(int signo) {
  if (signo == SIGUSR1) {
    received_sigusr1 = 1;
  }
}

/*------------------------------FILES THREAD---------------------------------*/

void *thread_operation(void *arg) {
  /*---------Blocking the signal----------*/
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGUSR1);
  pthread_sigmask(SIG_BLOCK, &set, NULL);
  /*--------------------------------------*/

  ThreadArgs *args = (ThreadArgs *)arg;
  char *dir_path = args->dir_path;

  struct dirent *dp;
  while (1) {

    safe_mutex_lock(&dir_lock);
    dp = readdir(dir);
    safe_mutex_unlock(&dir_lock);

    if (dp == NULL) { // If there are no more files to read
      break;
    }

    /*--------------------------OPENING .JOB FILE----------------------------*/

    if (dp->d_type == DT_REG && strlen(dp->d_name) > 3 &&
        strcmp(dp->d_name + strlen(dp->d_name) - 4, ".job") == 0) {
      int backups = 1;

      size_t len_path = strlen(dir_path) + 1 + strlen(dp->d_name) + 1;
      char *jobs_file_path = (char *)safe_malloc(len_path);
      snprintf(jobs_file_path, len_path, "%s/%s", dir_path, dp->d_name);

      int jobs_fd = open(jobs_file_path, O_RDONLY);
      if (jobs_fd == -1) {
        fprintf(stderr, "Failed to open .job file\n");
        free(jobs_file_path);
        return NULL;
      }
      /*JOB FILE OPENED*/

      /*------------------------OPENING .OUT FILE----------------------------*/

      char output_file_path[PATH_MAX];
      snprintf(output_file_path, sizeof(output_file_path), "%.*sout",
               (int)(strlen(jobs_file_path) - 3), jobs_file_path);

      int out_fd = open(output_file_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
      if (out_fd < 0) {
        fprintf(stderr, "Failed to create output file\n");
        free(jobs_file_path);
        return NULL;
      }

      /*------------------------ANALIZING COMMANDS---------------------------*/

      int should_exit = 0;
      while (!should_exit) {
        char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
        char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
        unsigned int delay;
        size_t num_pairs;

        switch (get_next(jobs_fd)) {
        case CMD_WRITE:
          num_pairs = parse_write(jobs_fd, keys, values, MAX_WRITE_SIZE,
                                  MAX_STRING_SIZE);
          if (num_pairs == 0) {
            fprintf(stderr, "Invalid command. See HELP for usage\n");
            continue;
          }

          if (kvs_write(num_pairs, keys, values, subs_list)) {
            fprintf(stderr, "Failed to write pair\n");
          }
          break;

        case CMD_READ:
          num_pairs =
              parse_read_delete(jobs_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
          if (num_pairs == 0) {
            fprintf(stderr, "Invalid command. See HELP for usage\n");
            continue;
          }

          if (kvs_read(num_pairs, keys, out_fd)) {
            fprintf(stderr, "Failed to read pair\n");
          }
          break;

        case CMD_DELETE:
          num_pairs =
              parse_read_delete(jobs_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
          if (num_pairs == 0) {
            fprintf(stderr, "Invalid command. See HELP for usage\n");
            continue;
          }

          if (kvs_delete(num_pairs, keys, out_fd, subs_list)) {
            fprintf(stderr, "Failed to delete pair\n");
          }
          break;

        case CMD_SHOW:
          kvs_show(out_fd);
          break;

        case CMD_WAIT:
          if (parse_wait(jobs_fd, &delay, NULL) == -1) {
            fprintf(stderr, "Invalid command. See HELP for usage\n");
            continue;
          }

          if (delay > 0) {
            write_to_file(out_fd, "Waiting...\n");
            kvs_wait(delay);
          }
          break;

        case CMD_BACKUP:
          lock_table();
          /*CHECKING IF MAX LIMIT OF CHILD PROCESS IS REACHED AND WAITING*/
          safe_mutex_lock(&active_child_mutex);
          if (active_child == MAX_PROC) {
            pid_t pid = wait(NULL);
            if (pid == -1) {
              fprintf(stderr, "wait failed\n");
            } else {
              active_child--; // Decrement active children only if a child
                              // process exits
            }
          }
          /*WAIT ENDED - CREATING CHILD PROCESS*/
          pid_t pid = fork();
          unlock_table();

          if (pid == -1) {
            fprintf(stderr, "Failed to fork\n");
            free(jobs_file_path);
            exit(1);
          }

          /*-------------------------CHILD PROCESS---------------------------*/

          if (pid == 0) {
            /*CREATING .BCK FILE*/
            safe_mutex_unlock(&active_child_mutex);
            char temp_path[MAX_JOB_FILE_NAME_SIZE];
            snprintf(temp_path, sizeof(temp_path), "%.*s",
                     (int)(strlen(jobs_file_path) - 4), jobs_file_path);

            char backup_file_path[PATH_MAX];
            snprintf(backup_file_path, sizeof(backup_file_path), "%s-%d.bck",
                     temp_path, backups);

            int bck_fd =
                open(backup_file_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
            if (bck_fd < 0) {
              fprintf(stderr, "Failed to create backup file: %s\n",
                      backup_file_path);
              exit(1); // Ensure child exits on error
            }
            /*OPENED .BCK FILE*/
            if (kvs_backup(bck_fd)) { // Performing the backup
              fprintf(stderr, "Failed to perform backup.\n");
              if (close(bck_fd) == -1) {
                fprintf(stderr, "Failed to close .bck file\n");
                return NULL;
              }
              free(jobs_file_path);
              exit(1);
            }

            free(jobs_file_path);
            if (close(bck_fd) == -1) {
              fprintf(stderr, "Failed to close .bck file\n");
              return NULL;
            }
            kvs_terminate();
            close(jobs_fd);
            close(out_fd);
            closedir(dir);
            exit(0); // Child successfully exits after performing the backup
          }
        /*-------------------------------------------------------------------*/

        /*-----------------------PARENT PROCESS JUMP-------------------------*/

          safe_mutex_unlock(&active_child_mutex);
          active_child++; // Increment active children count
          backups++;
          break;

        case CMD_INVALID:
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          break;

        case CMD_HELP:
          printf("Available commands:\n"
                 "  WRITE [(key,value),(key2,value2),...]\n"
                 "  READ [key,key2,...]\n"
                 "  DELETE [key,key2,...]\n"
                 "  SHOW\n"
                 "  WAIT <delay_ms>\n"
                 "  BACKUP\n"
                 "  HELP\n");
          break;

        case CMD_EMPTY:
          break;

        case EOC:
          should_exit = 1;
          break;
        }
      }

      if (close(jobs_fd) == -1) {
        fprintf(stderr, "Failed to close .jobs file\n");
        return NULL;
      }
      if (close(out_fd) == -1) {
        fprintf(stderr, "Failed to close .out file\n");
        return NULL;
      }
      free(jobs_file_path);
    }
  }
  return NULL;
}

/*------------------------------CLIENT THREAD--------------------------------*/

void ClientHandlerFunction() {

  /*---------Blocking the signal----------*/
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGUSR1);
  pthread_sigmask(SIG_BLOCK, &set, NULL);
  /*--------------------------------------*/

  while (1) {
    //Checking if client can connect
    sem_wait(&full);
    pthread_mutex_lock(&buffer_lock);
    Client client = clients[out];
    out = (out + 1) % MAX_SESSION_COUNT;
    pthread_mutex_unlock(&buffer_lock);
    sem_post(&empty);

    // Process the paths as needed
    int req_fd = safe_open(client.req_pipe_path, O_RDONLY);
    int resp_fd = safe_open(client.resp_pipe_path, O_WRONLY);
    int notif_fd = safe_open(client.notif_pipe_path, O_WRONLY);
    if (req_fd < 0 || notif_fd < 0) {
      write_response(resp_fd, OP_CODE_CONNECT, 1);
      return;
    }
    write_response(resp_fd, OP_CODE_CONNECT, 0);
    int connected = 1;

    //Add new client to the active clients list
    ActiveClient *new_client = safe_malloc(sizeof(ActiveClient));
    new_client->client_req_fd = req_fd;
    new_client->client_resp_fd = resp_fd;
    new_client->client_notif_fd = notif_fd;
    new_client->next = NULL;

    safe_mutex_lock(&active_clients_list->active_clients_lock);
    new_client->next = active_clients_list->head;
    active_clients_list->head = new_client;
    active_clients_list->active_clients_counter++;
    safe_mutex_unlock(&active_clients_list->active_clients_lock);

    int result;

    /*----------------------PROCESSING CLIENT REQUESTS-----------------------*/
    while (connected) {
      char OP_CODE;

      //Client was unexpectedly disconnected
      if (read_all(req_fd, &OP_CODE, 1, 0) != 1) {
        break;
      }

      char key[MAX_STRING_SIZE];
      switch (OP_CODE) {

      // DISCONNECT
      case 2:
        result = remove_all_subscriptions_from_client(subs_list, notif_fd);
        if (result == 1) {
          write_response(resp_fd, OP_CODE_DISCONNECT, 1);
          break;
        }
        if (safe_close(req_fd) == -1 || safe_close(notif_fd) == -1) {
          write_response(resp_fd, OP_CODE_DISCONNECT, 1);
          break;
        }
        write_response(resp_fd, OP_CODE_DISCONNECT, 0);
        safe_close(resp_fd);
        connected = 0;
        remove_active_client(active_clients_list, resp_fd);
        break;

      // SUBSCRIBE
      case 3:
        if (read_all(req_fd, key, MAX_STRING_SIZE, 0) != 1) {
          fprintf(stderr, "Failed to read key from request pipe\n");
          break;
        }

        result = add_subscription(subs_list, key, notif_fd);
        if (result == 0) {
          // Key doesn't exist, subscription failed
          write_response(resp_fd, OP_CODE_SUB, 0);
          // Key exists, subscription successful
        } else if (result == 1) {
          write_response(resp_fd, OP_CODE_SUB, 1);
          // Subscription failed unexpectedly
        } else {
          write_response(resp_fd, OP_CODE_SUB, 2);
        }
        break;

      // UNSUBSCRIBE
      case 4:
        if (read_all(req_fd, key, MAX_STRING_SIZE, 0) != 1) {
          fprintf(stderr, "Failed to read key from request pipe\n");
          break;
        }
        result = unsubscribe_from_key(subs_list, key, notif_fd);
        if (result == 0) {
          // Subscription removed successfully
          write_response(resp_fd, OP_CODE_UNSUB, 0);
          // Subscription doesn't exist, unsubscription failed
        } else if (result == 1) {
          write_response(resp_fd, OP_CODE_UNSUB, 1);
          // Unsubscription failed unexpectedly
        } else {
          write_response(resp_fd, OP_CODE_UNSUB, 2);
        }
        break;

      // UNKNOWN
      default:
        fprintf(stderr, "Unknown command received: %c\n", OP_CODE);
        break;
      }
    }
  }
}

/*-------------------------------HOST THREAD---------------------------------*/

void HostThreadFunction() {

  /*---------Signal Mask and Handler----------*/
  // Set up signal mask
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGUSR1);
  pthread_sigmask(SIG_UNBLOCK, &set, NULL);

  // Set up signal handler
  struct sigaction sa;
  memset(&sa, 0, sizeof(sa));
  sa.sa_handler = sig_handler;
  sa.sa_flags = 0;
  sigemptyset(&sa.sa_mask);
  if (sigaction(SIGUSR1, &sa, NULL) == -1) {
    fprintf(stderr, "Failed to set up signal handler\n");
    return;
  }
  /*------------------------------------------*/

  int pipe_fd = safe_open(pipe_name, O_RDWR);
  if (pipe_fd < 0) {
    fprintf(stderr, "Failed to open pipe\n");
    return;
  }

  while (1) {
    //Check if there was a SIGUSR1 signal
    pthread_mutex_lock(&sigusr1_mutex);
    if (received_sigusr1) {
      safe_mutex_lock(&active_clients_list->active_clients_lock);
      // Clean up all subscriptions and client connections
      free_subs_list(subs_list);
      disconnect_all_clients(active_clients_list);
      subs_list = create_subscription_list();
      pthread_mutex_unlock(&sigusr1_mutex);
      received_sigusr1 = 0;
      safe_mutex_unlock(&active_clients_list->active_clients_lock);
      // Continue accepting new connections
      continue;
    }
    pthread_mutex_unlock(&sigusr1_mutex);

    char msg[1 + MAX_PIPE_PATH_LENGTH * 3] = {0};
    int intr = received_sigusr1;

    ssize_t bytes_read = read_all(pipe_fd, msg, sizeof(msg), &intr);
    received_sigusr1 = intr;

    //Process connection requests
    if (bytes_read == 1) {
      if (msg[0] == OP_CODE_CONNECT) {
        Client client;
        strncpy(client.req_pipe_path, msg + 1, MAX_PIPE_PATH_LENGTH);
        strncpy(client.resp_pipe_path, msg + 1 + MAX_PIPE_PATH_LENGTH,
                MAX_PIPE_PATH_LENGTH);
        strncpy(client.notif_pipe_path, msg + 1 + 2 * MAX_PIPE_PATH_LENGTH,
                MAX_PIPE_PATH_LENGTH);

        sem_wait(&empty);
        pthread_mutex_lock(&buffer_lock);
        clients[in] = client;
        in = (in + 1) % MAX_SESSION_COUNT;
        pthread_mutex_unlock(&buffer_lock);
        sem_post(&full);
      } else {
        fprintf(stderr, "Unknown operation code received\n");
      }
    // Check if the read call was interrupted by the SIGUSR1 signal
    } else if (bytes_read < 0 && errno == EINTR) {
        if (received_sigusr1) {
          continue;
        }
      }
  }
  safe_close(pipe_fd);
}

/*----------------------------------MAIN------------------------------------*/

int main(int argc, char *argv[]) {
  /*---------Blocking the signal----------*/
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGUSR1);
  pthread_sigmask(SIG_BLOCK, &set, NULL);
  /*--------------------------------------*/

  if (argc != 5) {
    fprintf(
        stderr,
        "Usage: %s <dir_path> <MAX_PROC> <MAX_THREADS> <REGISTER_PIPE_NAME>\n",
        argv[0]);
    return 1;
  }

  if (kvs_init()) {
    fprintf(stderr, "Failed to initialize KVS\n");
    return 1;
  }

  subs_list = create_subscription_list();
  active_clients_list = create_active_clients_list();

  char *dir_path = argv[1];
  dir = opendir(dir_path);
  if (dir == NULL) {
    fprintf(stderr, "Failed to open directory\n");
    return 1;
  }

  if (sscanf(argv[2], "%d", &MAX_PROC) != 1) {
    fprintf(stderr, "Invalid number provided for MAX_PROC\n");
    return 1;
  }

  int MAX_THREADS = 0;
  if (sscanf(argv[3], "%d", &MAX_THREADS) != 1) {
    fprintf(stderr, "Invalid number provided for MAX_THREADS\n");
    return 1;
  }

  snprintf(pipe_name, sizeof(pipe_name), "/tmp/%s", argv[4]);
  if (safe_mkfifo(pipe_name, 0666) != 0) {
    fprintf(stderr, "Failed to register pipe\n");
    return 1;
  }
  if (MAX_THREADS <= 0) {
    fprintf(stderr, "Invalid number of threads: %d\n", MAX_THREADS);
    return 1;
  }
  
  pthread_t HostThread;
  pthread_t clientThreads[S];
  sem_init(&empty, 0, S);
  sem_init(&full, 0, 0);
  pthread_t threads[MAX_THREADS];
  int thread_created[MAX_THREADS];
  ThreadArgs args = {argv[1]};

  for (int i = 0; i < MAX_THREADS; i++) {
    thread_created[i] = 0;
  }
  if (pthread_create(&HostThread, NULL, (void *)HostThreadFunction, NULL) !=
      0) {
    fprintf(stderr, "Error creating HostThread\n");
  }
  for (int i = 0; i < S; i++) {
    if (pthread_create(&clientThreads[i], NULL, (void *)ClientHandlerFunction,
                       NULL) != 0) {
      fprintf(stderr, "Error creating client thread number: %d\n", i);
    }
  }
  for (int i = 0; i < MAX_THREADS; i++) {
    if (pthread_create(&threads[i], NULL, thread_operation, (void *)&args) !=
        0) {
      fprintf(stderr, "Error creating thread number: %d\n", i);
    } else {
      thread_created[i] = 1;
    }
  }

  for (int i = 0; i < MAX_THREADS; i++) {
    if (thread_created[i]) {
      if (pthread_join(threads[i], NULL) != 0) {
        fprintf(stderr, "Error joining thread number: %d\n", i);
      }
    }
  }
  for (int i = 0; i < S; i++) {
    if (pthread_join(clientThreads[i], NULL) != 0) {
      fprintf(stderr, "Error joining client thread number: %d\n", i);
    }
  }
  if (pthread_join(HostThread, NULL) != 0) {
    fprintf(stderr, "Error joining HostThread\n");
  }

  closedir(dir);
  free_subs_list(subs_list);
  kvs_terminate();

  /*WAITING FOR ALL THE BACKUPS TO FINISH*/
  while (active_child > 0) {
    if (wait(NULL) > 0) {
      active_child--;
    }
  }
  return 0;
}