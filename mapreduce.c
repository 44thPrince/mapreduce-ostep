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
        partition_function(key, num_partitions);

    if (partition_index >= (unsigned long)num_partitions) {
        return;
    }

    partition_t *partition = &partitions[partition_index];

    int bucket_index = h31_hash(key, sizeof(key)/sizeof(char));

    // Allocate the value node and independent value copy
    value_node_t *new_value = malloc(sizeof(value_node_t));

    if (new_value == NULL) {
        return;
    }

    strcpy(new_value->value, value);

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

    strcpy(new_key->key, key);

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

/*  Kevin Bernichio
    Member 3: compareKeys, sort_partition, and get_next
*/

// comparator used by both qsort and bsearch to put keys in ascending order
static int compareKeys(const void *a, const void *b)
{
    /*  - the sorted array holds key_entry_t pointers, so qsort and bsearch hand us
        a pointer to one of those pointers
        - strcmp compares byte by byte, so "" comes first and uppercase comes before lowercase
    */

    // dereference once to get the actual key entries we want to compare
    const key_entry_t *left = *(key_entry_t *const *)a;
    const key_entry_t *right = *(key_entry_t *const *)b;

    return strcmp(left->key, right->key);
}

// copies every key in a partition into an array and sorts it in ascending order
static void sort_partition(partition_t *p)
{
    /*  - called once per partition by that partition's reducer thread, after all of the mappers are done
        - only that one thread touches this partition from here on, so we don't need a lock
        - also points each key's cursor at its first value so get_next starts from the beginning
    */

    // declare and initialize our variables that we'll need later
    size_t keyCount = 0;
    size_t index = 0;
    key_entry_t *entry;

    // reset the index of the key being reduced
    p->current = 0;

    // count the keys by walking every bucket instead of trusting num_keys, so the array size always matches what's actually stored
    for(size_t bucket = 0; bucket < p->num_buckets; bucket++)
    {
        for(entry = p->buckets[bucket]; entry != NULL; entry = entry->next)
        {
            keyCount++;
        }
    }

    p->num_keys = keyCount;

    // an empty partition has nothing to sort, so sorted stays NULL
    if(keyCount == 0)
    {
        return;
    }

    // allocate room for one pointer per key
    p->sorted = malloc(keyCount * sizeof(key_entry_t *));

    // if malloc fails, set num_keys to 0 so the reducer loop just doesn't run
    if(p->sorted == NULL)
    {
        p->num_keys = 0;
        return;
    }

    // walk the buckets again, this time copying each key entry into the array
    for(size_t bucket = 0; bucket < p->num_buckets; bucket++)
    {
        for(entry = p->buckets[bucket]; entry != NULL; entry = entry->next)
        {
            // point the cursor at the first value so get_next starts at the beginning of the list
            entry->cursor = entry->head;

            p->sorted[index] = entry;
            index++;
        }
    }

    // sort the array of key entries in ascending order using our comparator
    qsort(p->sorted, p->num_keys, sizeof(key_entry_t *), compareKeys);
}

// the Getter passed to the user's Reduce(), returns the next value for a key
static char *get_next(char *key, int partition_number)
{
    /*  - returns the next value for the key, or NULL once every value has been returned
        - keeps returning NULL on every call after that, it doesn't wrap back around
        - each key's cursor remembers where it left off, so reducers working on
        different partitions can't mess with each other's iteration
    */

    // declare and/or initialize our variables that we'll need later
    partition_t *p;
    key_entry_t *entry = NULL;
    char *value;

    // reject a NULL key or a partition number that's out of range
    if(key == NULL || partition_number < 0 || partition_number >= num_partitions)
    {
        return NULL;
    }

    p = &partitions[partition_number];

    // an empty partition never got a sorted array, so there's nothing to return
    if(p->sorted == NULL)
    {
        return NULL;
    }

    /*  - fast path: check the key currently being reduced first, since that's almost always the one being asked for
        - comparing the pointers first is cheap because the reducer passes in the library's own key string
    */
    if(p->current < p->num_keys)
    {
        key_entry_t *currentEntry = p->sorted[p->current];

        if(currentEntry->key == key || strcmp(currentEntry->key, key) == 0)
        {
            entry = currentEntry;
        }
    }

    // slow path: it's some other key, so binary search the sorted array for it
    if(entry == NULL)
    {
        // bsearch needs the item we're searching for to look like an array element, which is a pointer to a key_entry_t
        key_entry_t probe;
        key_entry_t *probePtr = &probe;
        key_entry_t **found;

        probe.key = key;
        found = bsearch(&probePtr, p->sorted, p->num_keys, sizeof(key_entry_t *), compareKeys);

        // the key isn't in this partition
        if(found == NULL)
        {
            return NULL;
        }

        entry = *found;
    }

    // every value for this key has already been returned
    if(entry->cursor == NULL)
    {
        return NULL;
    }

    // save the current value, then move the cursor to the next node for the next call
    value = entry->cursor->value;
    entry->cursor = entry->cursor->next;

    // return the library's stored pointer, Member 4's cleanup frees it after reducing is done
    return value;
}

/* ===================== MEMBER 4: reduce phase + lifecycle ===================== */

/*  Diego Mendoza
    Member 4: reducer_worker, reduce_phase, store_destroy, and MR_Run
*/

typedef struct ReducerTask {
    int partition_number;
    pthread_t thread;
    int started;
} ReducerTask;

static void *reducer_worker(void *arg)
{
    const ReducerTask *task = arg;
    int partition_number = task->partition_number;
    partition_t *partition = &partitions[partition_number];

    // sort this partition once and set each key's cursor before reducing
    sort_partition(partition);
    for (partition->current = 0; partition->current < partition->num_keys;
         ++partition->current) {
        // current lets get_next find the key we're reducing, e.g. apple before banana
        key_entry_t *key = partition->sorted[partition->current];
        reduce_fn(key->key, get_next, partition_number);
    }
    return NULL;
}

static void reduce_phase(int num_reducers)
{
    // give each worker its own partition number and keep the tasks until all joins finish
    ReducerTask *tasks = calloc((size_t)num_reducers, sizeof(*tasks));
    // run the reducers on this thread if we can't allocate the task array
    if (tasks == NULL) {
        for (int i = 0; i < num_reducers; ++i) {
            ReducerTask task = { .partition_number = i };
            reducer_worker(&task);
        }
        return;
    }

    for (int i = 0; i < num_reducers; ++i) {
        tasks[i].partition_number = i;
        tasks[i].started = pthread_create(&tasks[i].thread, NULL,
                                          reducer_worker, &tasks[i]) == 0;
        // process this partition here if its thread couldn't start
        if (!tasks[i].started)
            reducer_worker(&tasks[i]);
    }
    for (int i = 0; i < num_reducers; ++i) {
        // wait for each worker so cleanup can safely free the keys and values
        if (tasks[i].started && pthread_join(tasks[i].thread, NULL) != 0)
            abort();
    }
    free(tasks);
}

static void store_destroy(void)
{
    for (int i = 0; i < num_partitions; ++i) {
        partition_t *partition = &partitions[i];
        // sorted holds pointers to the keys, and the buckets own the key nodes
        free(partition->sorted);
        for (size_t bucket = 0; bucket < (size_t)partition->num_buckets; ++bucket) {
            key_entry_t *key = partition->buckets[bucket];
            while (key != NULL) {
                key_entry_t *next_key = key->next;
                // start at head so we free every value, including ones the reducer didn't read
                value_node_t *value = key->head;
                while (value != NULL) {
                    value_node_t *next_value = value->next;
                    free(value->value);
                    free(value);
                    value = next_value;
                }
                free(key->key);
                free(key);
                key = next_key;
            }
        }
        free(partition->buckets);
        pthread_mutex_destroy(&partition->lock);
    }
    free(partitions);
    partitions = NULL;
    num_partitions = 0;
}

void MR_Run(int argc, char *argv[], Mapper map, int num_mappers,
            Reducer reduce, int num_reducers, Partitioner partition)
{
    if (num_reducers <= 0)
        return;

    map_fn = map;
    reduce_fn = reduce;
    partition_fn = partition != NULL ? partition : MR_DefaultHashPartition;
    num_partitions = num_reducers;

    // initialize storage, finish mapping, reduce in key order per partition, then clean up
    store_init(num_partitions);
    map_phase(argc, argv, num_mappers);
    reduce_phase(num_reducers);
    store_destroy();
}
