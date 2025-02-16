#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>

#include "kvs.h"
#include "operations.h"
#include "src/common/io.h"
#include "string.h"

/*---------------------------AUXILIARY FUNCTIONS-----------------------------*/

int hash(const char *key) {
  int firstLetter = tolower(key[0]);
  if (firstLetter >= 'a' && firstLetter <= 'z') {
    return firstLetter - 'a';
  } else if (firstLetter >= '0' && firstLetter <= '9') {
    return firstLetter - '0';
  }
  return -1; // Invalid index for non-alphabetic or number strings
}

void destroy_locks(HashTable *ht, int up_to_index) {
  for (int i = 0; i < up_to_index; i++) {
    pthread_rwlock_destroy(&ht->table[i].list_lock);
  }
}

/*----------------------------CREATE FUNCTIONS-------------------------------*/

struct HashTable *create_hash_table() {
  // Allocate memory for the hash table
  HashTable *ht = malloc(sizeof(HashTable));
  if (!ht) {
    fprintf(stderr, "Failed to allocate memory for hash table\n");
    return NULL;
  }

  // Initialize each bucket's list and its lock
  for (int i = 0; i < TABLE_SIZE; i++) {
    ht->table[i].head = NULL; // Set the head pointer to NULL

    if (pthread_rwlock_init(&ht->table[i].list_lock, NULL) != 0) {
      destroy_locks(ht, i);
      free(ht);
      return NULL;
    }
  }

  // Initialize the global lock
  if (pthread_rwlock_init(&ht->global_lock, NULL) != 0) {
    destroy_locks(ht, TABLE_SIZE);
    free(ht);
    return NULL;
  }
  return ht; // Successfully created hash table
}

SubscriptionList *create_subscription_list() {
  SubscriptionList *list = safe_malloc(sizeof(SubscriptionList));

  list->head = NULL;
  return list;
}

ActiveClientsList *create_active_clients_list() {
  ActiveClientsList *list = safe_malloc(sizeof(ActiveClientsList));

  list->head = NULL;
  return list;
}

/*-------------------------SUBSCRIPTION FUNCTIONS----------------------------*/

void remove_active_client(ActiveClientsList *list, int resp_fd) {
  safe_mutex_lock(&list->active_clients_lock);
  ActiveClient *current = list->head;
  ActiveClient *prev = NULL;
  while (current != NULL) {
    if (current->client_resp_fd == resp_fd) {
      if (prev == NULL) {
        list->head = current->next;
      } else {
        prev->next = current->next;
      }
      free(current);
      list->active_clients_counter--;
      break;
    }
    prev = current;
    current = current->next;
  }
  safe_mutex_unlock(&list->active_clients_lock);
}

int add_subscription(SubscriptionList *list, const char *key, int notif_fd) {
  safe_wrlock(&list->subs_lock); // Lock the list for thread safety

  Subscription *current = list->head;

  // Check if the key already exists in the list
  while (current != NULL) {
    if (strcmp(current->key, key) == 0) {
      // Key found, check if the subscriber is already there
      int *currentSubs = current->subscribers;
      for (int i = 0; i < current->subscriber_count; i++) {
        if (currentSubs[i] == notif_fd) {
          safe_rdwrunlock(&list->subs_lock);
          return 2; // Client already subscribed
        }
      }
      // Add the new subscriber to the array
      current->subscribers[current->subscriber_count] = notif_fd;
      current->subscriber_count++;
      safe_rdwrunlock(&list->subs_lock);
      return 1; // Successfully added subscriber
    }
    current = current->next;
  }
  if (key_exists((char *)key)) {
    Subscription *new_sub = safe_malloc(sizeof(Subscription));
    new_sub->key = strdup(key);
    if (!new_sub->key) {
      free(new_sub);
      safe_rdwrunlock(&list->subs_lock);
      return 2; // Memory allocation failure
    }
    new_sub->subscribers[0] = notif_fd;
    if (!new_sub->subscribers[0]) {
      free(new_sub->key);
      free(new_sub);
      safe_rdwrunlock(&list->subs_lock);
      return 2; // Memory allocation failure
    }
    new_sub->subscriber_count = 1;
    new_sub->next = NULL;

    new_sub->next = list->head; // Insert the new subscription at the head
    list->head = new_sub;
    safe_rdwrunlock(&list->subs_lock);
    return 1;
  }
  // a key nao existe
  safe_rdwrunlock(&list->subs_lock);
  return 0;
}

int remove_subscription_from_a_client(Subscription *subscription,
                                      int notif_fd) {
  for (int i = 0; i < subscription->subscriber_count; i++) {
    if (subscription->subscribers[i] == notif_fd) {
      for (int j = i; j < subscription->subscriber_count - 1; j++) {
        subscription->subscribers[j] = subscription->subscribers[j + 1];
      }
      subscription->subscriber_count--;
      return 1; // Successfully removed subscriber
    }
  }
  return 0;
}

void remove_all_subscriptions_from_key(SubscriptionList *list,
                                       const char *key) {
  safe_wrlock(&list->subs_lock);
  Subscription *current = list->head;
  Subscription *prev = NULL;

  // Traverse the list to find the subscription for the given key
  while (current != NULL) {
    if (strcmp(current->key, key) == 0) {
      int notif_fd;
      for (int i = 0; i < current->subscriber_count; i++) {
        notif_fd = current->subscribers[i];
        write_notification(notif_fd, key, NULL, 2);
      }
      if (prev == NULL) {
        // The node to be removed is the head of the list
        list->head = current->next;
      } else {
        // The node to be removed is in the middle or at the end
        prev->next = current->next;
      }
      free(current->key);
      free(current);
      safe_rdwrunlock(&list->subs_lock);
      return;
    }
    prev = current;
    current = current->next;
  }
  safe_rdwrunlock(&list->subs_lock);
}

int remove_all_subscriptions_from_client(SubscriptionList *list, int notif_fd) {
  safe_wrlock(&list->subs_lock);
  Subscription *current = list->head;
  while (current != NULL) {
    remove_subscription_from_a_client(current, notif_fd);
    current = current->next;
  }
  safe_rdwrunlock(&list->subs_lock);
  return 0;
}

int unsubscribe_from_key(SubscriptionList *list, const char *key,
                         int notif_fd) {
  safe_wrlock(&list->subs_lock); // Lock the list for thread safety
  Subscription *current = list->head;

  // Traverse the list to find the subscription for the given key
  while (current != NULL) {
    if (strcmp(current->key, key) == 0) {
      // Key found, search for the subscriber
      if (remove_subscription_from_a_client(current, notif_fd) == 1) {
        safe_rdwrunlock(&list->subs_lock);
        return 0;
      }
      safe_rdwrunlock(&list->subs_lock);
      return 1; // Subscriber not found
    }
    current = current->next;
  }
  safe_rdwrunlock(&list->subs_lock);
  return -1; // Key not found
}

/*-----------------------------KVS FUNCTIONS---------------------------------*/

int write_pair(HashTable *ht, SubscriptionList *sub_list, const char *key,
               const char *value) {
  int index = hash(key);
  KeyNode *keyNode = ht->table[index].head;
  // Search for the key node

  while (keyNode != NULL) {
    if (strcmp(keyNode->key, key) == 0) {
      free(keyNode->value);
      keyNode->value = strdup(value);
      safe_rdlock(&sub_list->subs_lock);
      Subscription *current = sub_list->head;
      while (current != NULL) {
        if (strcmp(current->key, key) == 0) {
          int notif_fd;
          for (int i = 0; i < current->subscriber_count; i++) {
            notif_fd = current->subscribers[i];
            // Send a notification to all subscribers
            write_notification(notif_fd, key, value, 1);
          }
        }
        current = current->next;
      }
      safe_rdwrunlock(&sub_list->subs_lock);
      return 0;
    }
    keyNode = keyNode->next; // Move to the next node
  }

  // Key not found, create a new key node
  keyNode = safe_malloc(sizeof(KeyNode));
  keyNode->key = strdup(key);            // Allocate memory for the key
  keyNode->value = strdup(value);        // Allocate memory for the value
  keyNode->next = ht->table[index].head; // Link to existing nodes
  ht->table[index].head =
      keyNode; // Place new key node at the start of the list
  return 0;
}

char *read_pair(HashTable *ht, const char *key) {
  int index = hash(key);
  KeyNode *keyNode = ht->table[index].head;
  char *value;

  while (keyNode != NULL) {
    if (strcmp(keyNode->key, key) == 0) {
      value = strdup(keyNode->value);
      return value; // Return copy of the value if found
    }
    keyNode = keyNode->next; // Move to the next node
  }
  return NULL; // Key not found
}

int delete_pair(HashTable *ht, SubscriptionList *sub_list, const char *key) {
  int index = hash(key);
  List *list = &ht->table[index];
  KeyNode *keyNode = list->head;
  KeyNode *prevNode = NULL;

  while (keyNode != NULL) {
    if (strcmp(keyNode->key, key) == 0) {
      if (prevNode == NULL) {
        // Node to delete is the first node in the list
        ht->table[index].head =
            keyNode->next; // Update the table to point to the next node
      } else {
        // Node to delete is not the first; bypass it
        prevNode->next =
            keyNode->next; // Link the previous node to the next node
      }

      free(keyNode->key);
      free(keyNode->value);
      free(keyNode);
      // Send a notification to all subscribers
      remove_all_subscriptions_from_key(sub_list, key);
      return 0;
    }
    prevNode = keyNode;      // Move prevNode to current node
    keyNode = keyNode->next; // Move to the next node
  }

  return 1;
}

void free_table(HashTable *ht) {
  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = ht->table[i].head;
    while (keyNode != NULL) {
      KeyNode *temp = keyNode;
      keyNode = keyNode->next;
      free(temp->key);
      free(temp->value);
      free(temp);
    }
    ht->table[i].head = NULL;
  }
  destroy_locks(ht, TABLE_SIZE);
  pthread_rwlock_destroy(&ht->global_lock);
  free(ht);
}

void disconnect_all_clients(ActiveClientsList *list) {

  ActiveClient *current = list->head;
  while (current) {
    ActiveClient *next = current->next;
    safe_close(current->client_req_fd);
    safe_close(current->client_resp_fd);
    safe_close(current->client_notif_fd);
    free(current);
    current = next;
  }
  list->active_clients_counter = 0;
}

void free_subs_list(SubscriptionList *list) {
  if (!list) {
    return;
  }

  safe_wrlock(&list->subs_lock);

  Subscription *current = list->head;
  while (current) {
    Subscription *next = current->next;
    // Free the dynamically allocated key
    if (current->key) {
      free(current->key);
    }
    // Free the current subscription node
    free(current);
    current = next;
  }
  safe_rdwrunlock(&list->subs_lock);
  pthread_rwlock_destroy(&list->subs_lock);
  free(list);
}
