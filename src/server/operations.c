#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>
#include <unistd.h>

#include "constants.h"
#include "kvs.h"
#include "operations.h"
#include "src/common/io.h"

static struct HashTable *kvs_table = NULL;

/// Calculates a timespec from a delay in milliseconds.
/// @param delay_ms Delay in milliseconds.
/// @return Timespec with the given delay.
static struct timespec delay_to_timespec(unsigned int delay_ms) {
  return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

/*---------------------------AUXILIARY FUNCTIONS-----------------------------*/

int write_to_file(int out_fd, const char *buffer) {
  size_t total_written = 0;
  size_t len = strlen(buffer);
  while (total_written <
         len) { // In case it doesn't write the entire buffer in one call
    ssize_t num_written =
        write(out_fd, buffer + total_written, len - total_written);
    if (num_written == -1) {
      perror("Failed to write to file");
      return 1;
    }
    total_written += (size_t)num_written;
  }
  return 0;
}

int *create_alphabetical_index(char keys[][MAX_STRING_SIZE], size_t num_pairs) {

  int *sorted_indexes = safe_malloc(num_pairs * sizeof(int));

  // Initialize the index array with original indexes
  for (size_t i = 0; i < num_pairs; i++) {
    sorted_indexes[i] = (int)i;
  }

  // Sort indexes based on key values using selection sort
  for (size_t i = 0; i < num_pairs - 1; i++) {
    size_t min_idx = i;

    // Find the index of the smallest key in the remaining unsorted portion
    for (size_t j = i + 1; j < num_pairs; j++) {
      if (strcasecmp(keys[sorted_indexes[j]], keys[sorted_indexes[min_idx]]) <
          0) {
        min_idx = j;
      }
    }

    // Swap the found minimum index with the current index
    if (min_idx != i) {
      int temp = sorted_indexes[i];
      sorted_indexes[i] = sorted_indexes[min_idx];
      sorted_indexes[min_idx] = temp;
    }
  }

  return sorted_indexes;
}

int printTable(int fd) {
  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = kvs_table->table[i].head;
    while (keyNode != NULL) {
      char buf[BUF_SIZE];
      snprintf(buf, sizeof(buf), "(%s, %s)\n", keyNode->key, keyNode->value);
      if (write_to_file(fd, buf)) {
        fprintf(stderr, "Error writing to file\n");
        safe_rdwrunlock(&kvs_table->global_lock);
        return 1;
      }
      keyNode = keyNode->next; // Move to the next node
    }
  }
  return 0;
}

/*-------------------------TABLE SETTERS/GETTERS-----------------------------*/

void lock_table() { safe_wrlock(&kvs_table->global_lock); }

void unlock_table() { safe_rdwrunlock(&kvs_table->global_lock); }

int key_exists(char *key) {
  safe_rdlock(&kvs_table->global_lock);
  int index = hash(key);
  KeyNode *keyNode = kvs_table->table[index].head;
  // Search for the key node
  while (keyNode != NULL) {
    if (strcmp(keyNode->key, key) == 0) {
      return 1;
    }
    keyNode = keyNode->next; // Move to the next node
  }
  return 0;
  safe_rdwrunlock(&kvs_table->global_lock);
}

/*-----------------------------SAFE FUNCTIONS--------------------------------*/

void *safe_malloc(size_t size) {
  void *ptr = malloc(size);
  if (ptr == NULL) {
    fprintf(stderr, "Failed to allocate memory\n");
    exit(1);
  }
  return ptr;
}
void safe_mutex_lock(pthread_mutex_t *mutex) {
  int result = pthread_mutex_lock(mutex);
  if (result != 0) {
    fprintf(stderr, "Failed to mutex lock\n");
    exit(1);
  }
}
void safe_mutex_unlock(pthread_mutex_t *mutex) {
  int result = pthread_mutex_unlock(mutex);
  if (result != 0) {
    fprintf(stderr, "Failed to mutex unlock\n");
    exit(1);
  }
}
void safe_rdlock(pthread_rwlock_t *rwlock) {
  int result = pthread_rwlock_rdlock(rwlock);
  if (result != 0) {
    fprintf(stderr, "Failed to read lock\n");
    exit(1);
  }
}
void safe_wrlock(pthread_rwlock_t *rwlock) {
  int result = pthread_rwlock_wrlock(rwlock);
  if (result != 0) {
    fprintf(stderr, "Failed to write lock\n");
    exit(1);
  }
}
void safe_rdwrunlock(pthread_rwlock_t *rwlock) {
  int result = pthread_rwlock_unlock(rwlock);
  if (result != 0) {
    fprintf(stderr, "Failed to read/write unlock\n");
    exit(1);
  }
}

/*----------------------------CLIENT FUNCTIONS-------------------------------*/

void write_response(int resp_fd, char op_code, char result) {
  // Prepare the message: OP_CODE followed by the result
  char message[2];
  message[0] = op_code;
  message[1] = result;

  // Write the message to the response pipe
  safe_write(resp_fd, message, 2);
}

void write_notification(int notif_fd, const char *key, const char *value,
                        int type) {
  // Prepare the message: OP_CODE followed by the result
  char output[1 + 2 * MAX_STRING_SIZE] = {0};
  switch (type) {
  case 1: // Key was changed
    output[0] = 1;
    strncpy(output + 1, key, MAX_STRING_SIZE);
    strncpy(output + 1 + MAX_STRING_SIZE, value, MAX_STRING_SIZE);
    safe_write(notif_fd, output, sizeof(output));
    break;

  case 2: // Key was deleted
    output[0] = 2;
    strncpy(output + 1, key, MAX_STRING_SIZE);
    strncpy(output + 1 + MAX_STRING_SIZE, "DELETED", MAX_STRING_SIZE);
    safe_write(notif_fd, output, sizeof(output));
    break;

  case 3: // Terminate notifications thread
    output[0] = 3;
    safe_write(notif_fd, output, sizeof(output));
    break;

  default: // Invalid type
    break;
  }
}

/*-------------------------------OPERATIONS----------------------------------*/

int kvs_init() {
  if (kvs_table != NULL) {
    fprintf(stderr, "KVS state has already been initialized\n");
    return 1;
  }

  kvs_table = create_hash_table();
  return kvs_table == NULL;
}

int kvs_terminate() {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  free_table(kvs_table);
  return 0;
}

// Modified write function to work with sorted indexes
int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE],
              char values[][MAX_STRING_SIZE], SubscriptionList *sub_list) {

  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  // Create sorted index array
  int *sorted_indexes = create_alphabetical_index(keys, num_pairs);
  if (sorted_indexes == NULL) {
    fprintf(stderr, "Failed to create sorted indexes\n");
    return 1;
  }

  int locked[TABLE_SIZE] = {0};
  safe_rdlock(&kvs_table->global_lock);

  // Perform write operations in alphabetical order
  for (size_t i = 0; i < num_pairs; i++) {
    int original_index = sorted_indexes[i];
    int hashed_index = hash(keys[original_index]);
    if (!locked[hashed_index]) {
      locked[hashed_index] = 1;
      safe_wrlock(&kvs_table->table[hashed_index].list_lock);
    }

    if (write_pair(kvs_table, sub_list, keys[original_index],
                   values[original_index]) != 0) {
      fprintf(stderr, "Failed to write keypair (%s,%s)\n", keys[original_index],
              values[original_index]);
    }
  }

  // Unlock all acquired read locks
  for (int i = TABLE_SIZE - 1; i >= 0; i--) {
    if (locked[i]) {
      safe_rdwrunlock(&kvs_table->table[i].list_lock);
    }
  }

  safe_rdwrunlock(&kvs_table->global_lock);
  free(sorted_indexes);
  return 0;
}

// Modified read function to work with sorted indexes
int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int out_fd) {

  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  // Create sorted index array
  int *sorted_indexes = create_alphabetical_index(keys, num_pairs);
  if (sorted_indexes == NULL) {
    fprintf(stderr, "Failed to create sorted indexes\n");
    return 1;
  }

  int locked[TABLE_SIZE] = {0};
  // Perform read operations in alphabetical order
  write_to_file(out_fd, "[");
  for (size_t i = 0; i < num_pairs; i++) {
    int original_index = sorted_indexes[i];
    int hashed_index = hash(keys[original_index]);
    if (!locked[hashed_index]) {
      locked[hashed_index] = 1;
      safe_rdlock(&kvs_table->table[hashed_index].list_lock);
    }

    char *result = read_pair(kvs_table, keys[original_index]);

    if (result == NULL) {
      char buf[MAX_WRITE_SIZE];
      snprintf(buf, sizeof(buf), "(%s,KVSERROR)", keys[original_index]);
      write_to_file(out_fd, buf);
    }

    else {
      char buf[BUF_SIZE];
      snprintf(buf, sizeof(buf), "(%s,%s)", keys[original_index], result);
      write_to_file(out_fd, buf);
      free(result);
    }
  }

  write_to_file(out_fd, "]\n");
  for (int i = 0; i < TABLE_SIZE; i++) {
    if (locked[i]) {
      safe_rdwrunlock(&kvs_table->table[i].list_lock);
    }
  }
  free(sorted_indexes);
  return 0;
}

// Modified delete function to work with sorted indexes
int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int out_fd,
               SubscriptionList *sub_list) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  // Create sorted index array
  int *sorted_indexes = create_alphabetical_index(keys, num_pairs);
  if (sorted_indexes == NULL) {
    fprintf(stderr, "Failed to create sorted indexes\n");
    return 1;
  }

  safe_rdlock(&kvs_table->global_lock);
  int locked[TABLE_SIZE] = {0};
  // Perform delete operations in alphabetical order
  int aux = 0;
  for (size_t i = 0; i < num_pairs; i++) {
    int original_index = sorted_indexes[i];
    int hashed_index = hash(keys[original_index]);
    if (!locked[hashed_index]) {
      locked[hashed_index] = 1;
      safe_wrlock(&kvs_table->table[hashed_index].list_lock);
    }
    if (delete_pair(kvs_table, sub_list, keys[original_index]) != 0) {
      if (!aux) {
        write_to_file(out_fd, "[");
        aux = 1;
      }
      char buf[BUF_SIZE];
      snprintf(buf, sizeof(buf), "(%s,KVSMISSING)", keys[original_index]);
      write_to_file(out_fd, buf);
    }
  }
  if (aux) {
    write_to_file(out_fd, "]\n");
  }
  for (int i = 0; i < TABLE_SIZE; i++) {
    if (locked[i]) {
      safe_rdwrunlock(&kvs_table->table[i].list_lock);
    }
  }
  safe_rdwrunlock(&kvs_table->global_lock);

  free(sorted_indexes);
  return 0;
}

int kvs_show(int out_fd) {
  lock_table();
  printTable(out_fd);
  unlock_table();
  return 0;
}

int kvs_backup(int bck_fd) {
  if (printTable(bck_fd))
    return 1;
  return 0;
}

void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}