#ifndef KVS_OPERATIONS_H
#define KVS_OPERATIONS_H
#define BUF_SIZE 256

#include <pthread.h>
#include <stddef.h>

#include "constants.h"
#include "kvs.h"

/*---------------------------AUXILIARY FUNCTIONS-----------------------------*/

/// Writes the given buffer to a file descriptor, ensuring all bytes are
/// written. If writing fails, an error message is printed to stderr using
/// perror.
/// @param out_fd The file descriptor to write to.
/// @param buffer The string buffer to write.
/// @return 0 if the buffer is written successfully, 1 otherwise.
int write_to_file(int out_fd, const char *buf);

/// Creates an array of indices that sorts the keys in alphabetical order.
/// Sorting is case-insensitive.
/// @param keys Array of key strings to sort.
/// @param num_pairs Number of keys in the array.
/// @return Pointer to the dynamically allocated array of sorted indices,
///         or NULL if memory allocation fails.
int *create_alphabetical_index(char keys[][MAX_STRING_SIZE], size_t num_pairs);

/// Prints the contents of the key-value store's table to the specified output
/// file. Each key-value pair is written in the format "(key, value)", followed
/// by a newline. The function iterates over the entire table and writes the
/// data to the provided file descriptor.
/// @param fd File descriptor to which the key-value pairs will be written.
/// @return 0 on success, or 1 if there is an error writing to the file.
int printTable(int fd);

/*-------------------------TABLE SETTERS/GETTERS-----------------------------*/

/// Acquires a lock on the global table to ensure thread-safe operations.
/// This function must be called before performing any operations
/// that modify or access the table shared between threads.
/// The lock will block if another thread currently holds it.
/// @note Ensure to release the lock after use to avoid deadlocks.
void lock_table();

/// Releases the lock on the global table to allow other threads to access it.
/// This function must be called after a successful call to `lock_table`
/// to ensure proper synchronization and avoid deadlocks.
/// @note Ensure that every `lock_table` call is paired with a corresponding 
///       `unlock_table` call.
void unlock_table();

/// Checks if the given key exists in the hashtable.
/// This function searches for the specified key in the global hashtable
/// and determines whether it is currently stored.
/// @param key The key to search for in the hashtable. Must be a null-terminated 
///            string.
/// @return 1 if the key exists, 0 otherwise.
/// @note The caller must ensure that the hashtable is locked for reading or 
///       writing when calling this function to maintain thread safety.
int key_exists(char *key);

/*-----------------------------SAFE FUNCTIONS--------------------------------*/

/// Allocates memory of the given size and ensures it is successfully allocated.
/// If the allocation fails, the program terminates with an error message.
/// @param size Size of the memory block to allocate, in bytes.
/// @return Pointer to the allocated memory block.
void *safe_malloc(size_t size);

/// Locks the specified mutex, ensuring the operation succeeds.
/// If the lock operation fails, the program terminates with an error message.
/// @param mutex Pointer to the mutex to be locked.
void safe_mutex_lock(pthread_mutex_t *mutex);

/// Unlocks the specified mutex, ensuring the operation succeeds.
/// If the unlock operation fails, the program terminates with an error message.
/// @param mutex Pointer to the mutex to be unlocked.
void safe_mutex_unlock(pthread_mutex_t *mutex);

/// Acquires a read lock on the specified read-write lock, ensuring the
/// operation succeeds. If the lock operation fails, the program terminates with
/// an error message.
/// @param rwlock Pointer to the read-write lock to be locked for reading.
void safe_rdlock(pthread_rwlock_t *rwlock);

/// Acquires a write lock on the specified read-write lock, ensuring the
/// operation succeeds. If the lock operation fails, the program terminates with
/// an error message.
/// @param rwlock Pointer to the read-write lock to be locked for writing.
void safe_wrlock(pthread_rwlock_t *rwlock);

/// Releases a write lock on the specified read-write lock, ensuring the
/// operation succeeds. If the unlock operation fails, the program terminates
/// with an error message.
/// @param rwlock Pointer to the read-write lock to be unlocked from writing.
void safe_rdwrunlock(pthread_rwlock_t *rwlock);

/*----------------------------CLIENT FUNCTIONS-------------------------------*/

/// Writes a message to the client's response pipe file descriptor.
/// The response includes an operation code and a result value, formatted as 
/// a 2-byte message. If the write operation fails, an error message is printed, 
/// and the program terminates.
/// @param resp_fd File descriptor of the response pipe to write to.
/// @param op_code Operation code to identify the type of response (1 byte).
/// @param result Result of the operation.
void write_response(int resp_fd, char op_code, char result);

/// Sends a notification to the client's pipe with the provided key, value, 
/// and type.
/// The type determines whether it's an update (1), delete (2), or 
/// termination (3).
/// If the operation fails, the program will terminate.
/// @param notif_fd The notification pipe's file descriptor.
/// @param key The key to notify about (max 40 chars).
/// @param value The value related to the key (max 40 chars). NULL for deletions
///              and termination.
/// @param type 0 for update, 1 for delete, 2 for termination.
void write_notification(int notif_fd, const char *key, const char *value, 
                        int type);

/*-------------------------------OPERATIONS----------------------------------*/

/// Initializes the KVS state.
/// @return 0 if the KVS state was initialized successfully, 1 otherwise.
int kvs_init();

/// Destroys the KVS state.
/// @return 0 if the KVS state was terminated successfully, 1 otherwise.
int kvs_terminate();

/// Writes a key value pair to the KVS. If key already exists it is updated.
/// @param num_pairs Number of pairs being written.
/// @param keys Array of keys' strings.
/// @param values Array of values' strings.
/// @return 0 if the pairs were written successfully, 1 otherwise.
int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE],
              char values[][MAX_STRING_SIZE], SubscriptionList *sub_list);

/// Reads values from the KVS.
/// @param num_pairs Number of pairs to read.
/// @param keys Array of keys' strings.
/// @param fd File descriptor to write the (successful) output.
/// @return 0 if the key reading, 1 otherwise.
int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd);

/// Deletes key value pairs from the KVS.
/// @param num_pairs Number of pairs to read.
/// @param keys Array of keys' strings.
/// @return 0 if the pairs were deleted successfully, 1 otherwise.
int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd, 
    SubscriptionList *sub_list);

/// Writes the state of the KVS.
/// @param fd File descriptor to write the output.
/// @return 0 if the backup was successful, 1 otherwise.
int kvs_show(int fd);

/// Creates a backup of the KVS state and stores it in the correspondent
/// backup file
/// @param fd File descriptor to write the output.
/// @return 0 if the backup was successful, 1 otherwise.
int kvs_backup(int bck_fd);

/// Waits for the last backup to be called.
void kvs_wait_backup();

/// Waits for a given amount of time.
/// @param delay_us Delay in milliseconds.
void kvs_wait(unsigned int delay_ms);

#endif // KVS_OPERATIONS_H
