#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "mapreduce.h"

/* ===================== SHARED: do not edit without team OK ===================== */

typedef struct value_node {
    char *value;                /* library's own copy of the emitted value */
    struct value_node *next;
} value_node_t;

typedef struct key_entry {
    char *key;                  /* library's own copy of the key */
    value_node_t *head;         /* all values emitted for this key */
    value_node_t *cursor;       /* next value get_next() will return */
    struct key_entry *next;     /* next entry in the same hash bucket */
} key_entry_t;

typedef struct partition {
    pthread_mutex_t lock;       /* held by MR_Emit while touching this partition */
    key_entry_t **buckets;      /* hash table used during the map phase */
    size_t num_buckets;
    size_t num_keys;            /* unique keys in this partition */
    key_entry_t **sorted;       /* built by sort_partition(), ascending by key */
    size_t current;             /* index in sorted[] of the key being reduced */
} partition_t;

static partition_t *partitions;     /* array of num_partitions */
static int num_partitions;          /* always equal to num_reducers */
static Partitioner partition_fn;
static Mapper map_fn;
static Reducer reduce_fn;

static void store_init(int n);                               /* Member 1 */
static void map_phase(int argc, char *argv[], int num_mappers); /* Member 2 */
static void sort_partition(partition_t *p);                  /* Member 3 */
static char *get_next(char *key, int partition_number);      /* Member 3 */
static void reduce_phase(int num_reducers);                  /* Member 4 */
static void store_destroy(void);                             /* Member 4 */
unsigned int h31_hash(const char* s, size_t len);

/* ===================== MEMBER 1: store + MR_Emit ===================== */

static void store_init(int n) {
    partitions = malloc(sizeof(partition_t) * n);
    int num_buckets = 100; // Change if needed
    for (int i = 0; i < n; i++) {
        partitions[i] = (partition_t) {
            .lock = PTHREAD_MUTEX_INITIALIZER,
            .num_buckets = num_buckets,
            .buckets = calloc(partitions[i].num_buckets,sizeof(key_entry_t)),
            .num_keys = 0,
            .sorted = NULL,
            .current = 0
        };
    }
}

void MR_Emit(char *key, char *value) {
    if (key == NULL || value == NULL) {
        return;
    }

    if (partitions == NULL ||
        num_partitions <= 0 ||
        partition_fn == NULL) {
        return;
    }

    // Determine destination partition
    unsigned long partition_index =
        partition_fn(key, num_partitions);

    if (partition_index >= (unsigned long)num_partitions) {
        return;
    }

    partition_t *partition = &partitions[partition_index];

    int bucket_index = h31_hash(key, strlen(key));

    // Allocate the value node and independent value copy
    value_node_t *new_value = malloc(sizeof(value_node_t));

    if (new_value == NULL) {
        return;
    }

    new_value->value = strdup(value);

    if (new_value->value == NULL) {
        free(new_value);
        return;
    }

    new_value->next = NULL;

    // Lock the partition before modifying shared storage
    int lock_result = pthread_mutex_lock(&partition->lock);

    if (lock_result != 0) {
        free(new_value->value);
        free(new_value);
        return;
    }

    // Check if the key exists
    key_entry_t *current = partition->buckets[bucket_index];

    while (current != NULL) {
        if (strcmp(current->key, key) == 0) {
            // If there is a duplicate key then append the new value at the end of the list
            value_node_t *tmp = current->head;
            while (tmp != NULL) { // Find end of list
                tmp = tmp->next;
            }
            tmp->next = new_value;

            pthread_mutex_unlock(&partition->lock);

            return;
        }

        current = current->next;
    }

    // If the key does not exist then create a new entry
    key_entry_t *new_key = malloc(sizeof(key_entry_t));

    if (new_key == NULL) {
        pthread_mutex_unlock(&partition->lock);

        free(new_value->value);
        free(new_value);

        return;
    }

    new_key->key = strdup(key);

    if (new_key->key == NULL) {
        pthread_mutex_unlock(&partition->lock);

        free(new_key);
        free(new_value->value);
        free(new_value);

        return;
    }

    new_key->head = new_value;

    // Insert at the beginning of the bucket's linked list
    new_key->next = partition->buckets[bucket_index];

    partition->buckets[bucket_index] = new_key;

    // Release the mutex 
    pthread_mutex_unlock(&partition->lock);
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

/* ===================== MEMBER 2: map phase ===================== */

static void map_phase(int argc, char *argv[], int num_mappers) {
    /* TODO(M2) */
    (void)argc; (void)argv; (void)num_mappers;
}

/* ===================== MEMBER 3: sorting + getter ===================== */

static void sort_partition(partition_t *p) {
    /* TODO(M3) */
    (void)p;
}

static char *get_next(char *key, int partition_number) {
    /* TODO(M3) */
    (void)key; (void)partition_number;
    return NULL;
}

/* ===================== MEMBER 4: reduce phase + lifecycle ===================== */

static void reduce_phase(int num_reducers) {
    /* TODO(M4): remove these two lines once reducer threads use them */
    (void)sort_partition; (void)get_next;
    (void)num_reducers;
}

static void store_destroy(void) {
    /* TODO(M4) */
}

void MR_Run(int argc, char *argv[],
            Mapper map, int num_mappers,
            Reducer reduce, int num_reducers,
            Partitioner partition) {
    /* TODO(M4): this is the intended order; fill in details */
    map_fn = map;
    reduce_fn = reduce;
    partition_fn = partition;
    num_partitions = num_reducers;

    store_init(num_partitions);
    map_phase(argc, argv, num_mappers);
    reduce_phase(num_reducers);
    store_destroy();
}