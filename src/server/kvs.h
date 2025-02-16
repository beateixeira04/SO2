#ifndef KEY_VALUE_STORE_H
#define KEY_VALUE_STORE_H

#define TABLE_SIZE 26

#include "constants.h"
#include <pthread.h>
#include <stddef.h>

/*---------------------------------STRUCTS-----------------------------------*/

typedef struct KeyNode {
  char *key;
  char *value;
  struct KeyNode *next;
} KeyNode;

typedef struct List {
  KeyNode *head;
  pthread_rwlock_t list_lock;
} List;

typedef struct HashTable {
  List table[TABLE_SIZE];
  pthread_rwlock_t global_lock;
} HashTable;

typedef struct Subscription {
  char *key;
  int subscribers[10 * S];
  int subscriber_count;
  struct Subscription *next;
} Subscription;

typedef struct SubscriptionList {
  Subscription *head;
  pthread_rwlock_t subs_lock;
} SubscriptionList;

typedef struct ActiveClient {
  int client_req_fd;
  int client_resp_fd;
  int client_notif_fd;
  struct ActiveClient *next;
} ActiveClient;

typedef struct ActiveClientsList {
  ActiveClient *head;
  int active_clients_counter;
  pthread_mutex_t active_clients_lock;
} ActiveClientsList;

/*---------------------------AUXILIARY FUNCTIONS-----------------------------*/

// Hash function based on key initial.
// @param key Lowercase alphabetical string.
// @return hash.
// NOTE: This is not an ideal hash function, but is useful for test purposes of
// the project
int hash(const char *key);

/// Destroys the locks associated with the hash table up to the given index.
/// @param ht Pointer to the hash table whose locks will be destroyed.
/// @param up_to_index The index up to which the locks should be destroyed.
/// This function will iterate over the hash table up to the specified index
/// and release any locks held by the table entries.
void destroy_locks(HashTable *ht, int up_to_index);

/*---------------------------CREATION FUNCTIONS------------------------------*/

/// Creates a new event hash table.
/// @return Newly created hash table, NULL on failure
struct HashTable *create_hash_table();

/// Creates and initializes a SubscriptionList.
/// @return Pointer to the newly created SubscriptionList, or NULL on failure.
SubscriptionList *create_subscription_list();

/// Creates and initializes an ActiveClientsList.
/// @return Pointer to the newly created ActiveClientsList, or NULL on failure.
ActiveClientsList *create_active_clients_list();

/*-------------------------SUBSCRIPTION FUNCTIONS----------------------------*/

/// Removes an active client from the list based on the response file
/// descriptor.
/// @param list Pointer to the ActiveClientsList.
/// @param resp_fd Response file descriptor of the client to be removed.
void remove_active_client(ActiveClientsList *list, int resp_fd);

/// Adds a subscription to the subscription list.
/// @param list Pointer to the SubscriptionList.
/// @param key Subscription key to be added.
/// @param notif_fd Notification file descriptor associated with the
/// subscription.
/// @return 1 if the subscription was added successfully, 0 if the key doesn't
///         exist and 2 if there was an unexpected error.
int add_subscription(SubscriptionList *list, const char *key, int notif_fd);

/// Removes a notification file descriptor from a subscription.
/// @param subscription Pointer to the Subscription object from which the file
/// descriptor will be removed.
/// @param notif_fd Notification file descriptor to be removed from the
/// subscription.
/// @return 1 if the notification file descriptor was removed successfully, 0
/// otherwise.
int remove_subscription_from_a_client(Subscription *subscription, int notif_fd);

/// Removes all subscriptions associated with a specific key from the 
/// subscription list.
/// @param list Pointer to the SubscriptionList containing the subscriptions.
/// @param key Key for which all associated subscriptions will be removed
void remove_all_subscriptions_from_key(SubscriptionList *list, const char *key);

/// Removes all subscriptions associated with a specific client from the 
/// subscription list.
/// @param list Pointer to the SubscriptionList containing the subscriptions.
/// @param notif_fd File descriptor identifying the client whose subscriptions 
///                 will be removed.
/// @return 0 if the subscriptions were removed successfully
int remove_all_subscriptions_from_client(SubscriptionList *list, int notif_fd);

/// Unsubscribes a client from a specific key in the subscription list.
/// @param list Pointer to the SubscriptionList containing the subscriptions.
/// @param key The key to unsubscribe the client from.
/// @param notif_fd File descriptor identifying the client to unsubscribe.
/// @return 0 if the client was successfully unsubscribed, 1 if the subscription
///         doesn't exist and -1 if the key doesn't exist
int unsubscribe_from_key(SubscriptionList *list, const char *key, int notif_fd);

/*-----------------------------KVS FUNCTIONS---------------------------------*/

/// Appends a new key value pair to the hash table.
/// @param ht Hash table to be modified.
/// @param key Key of the pair to be written.
/// @param value Value of the pair to be written.
/// @return 0 if the node was appended successfully, 1 otherwise.
int write_pair(HashTable *ht, SubscriptionList *list, const char *key,
               const char *value);

/// Deletes the value of given key.
/// @param ht Hash table to delete from.
/// @param key Key of the pair to be deleted.
/// @return 0 if the node was deleted successfully, 1 otherwise.
char *read_pair(HashTable *ht, const char *key);

/// Appends a new node to the list.
/// @param list Event list to be modified.
/// @param key Key of the pair to read.
/// @return 0 if the node was appended successfully, 1 otherwise.
int delete_pair(HashTable *ht, SubscriptionList *list, const char *key);

/// Frees the hashtable.
/// @param ht Hash table to be deleted.
void free_table(HashTable *ht);
 
/// Frees the subscription list.
/// @param list Subscription list to be deleted.
void disconnect_all_clients(ActiveClientsList *list);

/// Frees the subscription list.
/// @param list Subscription list to be deleted.
void free_subs_list(SubscriptionList *list);

#endif // KVS_H
