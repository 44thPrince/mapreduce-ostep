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
    /* TODO(M1) */
    (void)key; (void)value;
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