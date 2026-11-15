#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <errno.h>
#include "mapreduce.h"

// Globals

static Partition *partitions;
static int num_partitions;
static Partitioner partition_function = MR_DefaultHashPartition(); // Make sure this works

// Hash Table

typedef struct ValueNode {
    char *value;
    struct ValueNode *next;
} ValueNode;

typedef struct KeyNode {
    char *key;
    ValueNode *values;
    ValueNode *last_value;
    struct KeyNode *next;
} KeyNode;

typedef struct Partition {
    KeyNode **buckets; // Inner hash table
    int num_buckets;
    pthread_mutex_t mutex;
} Partition;

// Value nodes must be freed starting from the end of the list
void freeValueNodes(*ValueNode valueNode) {
    if (valueNode->next == NULL) { // Base case
        free(valueNode->value);
        free(valueNode);
    }
    // Recursive case
    freeValueNodes(valueNode->next);
    free(valueNode->value);
    free(valueNode->next);
    free(valueNode);
}


void freeKeyNode(*KeyNode keyNode) {
    free(keyNode->key);
    freeValueNodes(values);
    free(keyNode);
}

// Partitions refers to the variable partitions, the individual partitions will be freed by freeSinglePartition
void freePartitions() {
    // Paritions can be freed in order because the number of them is known, allowing for indexing
    for (int i = 0; i < num_partitions; i++) {
        freeSinglePartition(partitions[i]);
    }
    free (partitions);
}


void freeSinglePartition(*Partition partition) {
    // Key nodes can be freed in order because the number of them is known, allowing for indexing
    for (int i = 0; i < partition.num_buckets) {
        freeKeyNode(partition->buckets[i]);
    }
    free(partition);
}

// *partitions must be initialized by MR_Run()
// If key or value are null the function will error
void MR_Emit(char* key, char* value) {
    if (key == NULL || value == NULL) {
        return; // Should error
    }

    if (partitions == NULL ||
        num_partitions <= 0 ||
        partition_function == NULL) {
        return; // Should error
    }

    // Determine destination partition
    unsigned long partition_index =
        partition_function(key, num_partitions);

    if (partition_index >= (unsigned long)num_partitions) {
        return; // Should error
    }

    Partition *partition = &partitions[partition_index];

    int bucket_index = MR_DefaultHashPartition(*key, num_buckets);
    
    // Allocate the value node and independent value copy
    ValueNode *new_value = malloc(sizeof(ValueNode));

    if (new_value == NULL) {
        return; // Should error
    }

    new_value->value = copy_string(value);

    if (new_value->value == NULL) {
        free(new_value);
        return; // Should error
    }

    new_value->next = NULL;

    // Lock the partition before modifying shared storage
    int lock_result = pthread_mutex_lock(&partition->mutex);

    if (lock_result != 0) {
        free(new_value->value);
        free(new_value);
        return; // Should error
    }

    // Check if the key exists
    KeyNode *current = partition->buckets[bucket_index];

    while (current != NULL) {
        if (strcmp(current->key, key) == 0) {
            // If there is a duplicate key then append the new value using separate chaining
            current->last_value->next = new_value;
            current->last_value = new_value;

            pthread_mutex_unlock(&partition->mutex);

            return; // Should error
        }

        current = current->next;
    }

    // If the key does not exist then create a new entry
    KeyNode *new_key = malloc(sizeof(KeyNode));

    if (new_key == NULL) {
        pthread_mutex_unlock(&partition->mutex);

        free(new_value->value);
        free(new_value);

        return; // Should error
    }

    new_key->key = copy_string(key);

    if (new_key->key == NULL) {
        pthread_mutex_unlock(&partition->mutex);

        free(new_key);
        free(new_value->value);
        free(new_value);

        return; // Should error
    }

    new_key->values = new_value;
    new_key->last_value = new_value;

    // Insert at the beginning of the bucket's linked list
    new_key->next = partition->buckets[bucket_index];

    partition->buckets[bucket_index] = new_key;

    // Release the mutex 
    pthread_mutex_unlock(&partition->mutex);
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}