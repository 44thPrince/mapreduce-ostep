/*  Kevin Bernichio
    PA2 - Map Reduce (Member 3)
    COP4600 Operating Systems
    test_sort.c
*/

/*  - unit test for sort_partition and get_next
    - includes mapreduce.c directly so the static functions are visible to us
    - builds the partitions by hand so this doesn't depend on MR_Emit

    - build and run from the repo root:
        gcc -Wall -Werror -pthread -O -I. -o tests/test_sort tests/test_sort.c
        ./tests/test_sort
*/
#include "../mapreduce.c"

// keeps track of how many checks failed so main can return the right exit code
static int failures = 0;

// prints the line number and the condition that failed instead of stopping the whole test
#define CHECK(cond)                                                     \
    do                                                                  \
    {                                                                   \
        if(!(cond))                                                     \
        {                                                               \
            fprintf(stderr, "FAIL line %d: %s\n", __LINE__, #cond);     \
            failures++;                                                 \
        }                                                               \
    } while(0)

// helper function that adds a key/value pair to a partition, similar to what MR_Emit does
static void addValue(partition_t *p, const char *key, const char *value)
{
    /*  - picks a bucket using the first character of the key
        - creates the key entry if the key is new, otherwise reuses it
        - adds values to the end of the list so the test can check them in order
    */

    // declare and initialize our variables that we'll need later
    size_t bucket = (unsigned char)key[0] % p->num_buckets;
    key_entry_t *entry;
    value_node_t *newValue = malloc(sizeof(value_node_t));

    newValue->value = strdup(value);
    newValue->next = NULL;

    // look through the bucket's chain to see if the key already exists
    for(entry = p->buckets[bucket]; entry != NULL; entry = entry->next)
    {
        if(strcmp(entry->key, key) == 0)
        {
            break;
        }
    }

    // if the key doesn't exist yet, create a new entry and put it at the front of the chain
    if(entry == NULL)
    {
        entry = calloc(1, sizeof(key_entry_t));
        entry->key = strdup(key);
        entry->next = p->buckets[bucket];
        p->buckets[bucket] = entry;
        p->num_keys++;
    }

    // first value for this key becomes the head, otherwise walk to the end of the list and add it there
    if(entry->head == NULL)
    {
        entry->head = newValue;
    }
    else
    {
        value_node_t *last = entry->head;

        while(last->next != NULL)
        {
            last = last->next;
        }

        last->next = newValue;
    }
}

// creates numPartitions empty partitions, each with numBuckets buckets
static void setupPartitions(int numPartitions, size_t numBuckets)
{
    partitions = calloc(numPartitions, sizeof(partition_t));
    num_partitions = numPartitions;

    for(int i = 0; i < numPartitions; i++)
    {
        partitions[i].num_buckets = numBuckets;
        partitions[i].buckets = calloc(numBuckets, sizeof(key_entry_t *));
    }
}

// frees everything setupPartitions and addValue allocated so Valgrind stays clean
static void freePartitions(void)
{
    for(int i = 0; i < num_partitions; i++)
    {
        partition_t *p = &partitions[i];

        // free every key entry in every bucket, along with all of its values
        for(size_t bucket = 0; bucket < p->num_buckets; bucket++)
        {
            key_entry_t *entry = p->buckets[bucket];

            while(entry != NULL)
            {
                key_entry_t *nextEntry = entry->next;
                value_node_t *node = entry->head;

                while(node != NULL)
                {
                    value_node_t *nextNode = node->next;

                    free(node->value);
                    free(node);
                    node = nextNode;
                }

                free(entry->key);
                free(entry);
                entry = nextEntry;
            }
        }

        free(p->buckets);
        free(p->sorted);
    }

    free(partitions);
    partitions = NULL;
    num_partitions = 0;
}

// keys should come out in strcmp order, including "", mixed case, and prefixes like "a" and "ab"
static void testSortedOrder(void)
{
    // declare and initialize the keys we'll insert and the order we expect them back in
    const char *keys[] = { "banana", "a", "", "Zebra", "ab", "apple", "b", "AB" };
    const char *expected[] = { "", "AB", "Zebra", "a", "ab", "apple", "b", "banana" };
    size_t numKeys = sizeof(keys) / sizeof(keys[0]);

    setupPartitions(1, 3);

    for(size_t i = 0; i < numKeys; i++)
    {
        addValue(&partitions[0], keys[i], "1");
    }

    // set num_keys to 0 on purpose, since sort_partition shouldn't rely on it
    partitions[0].num_keys = 0;

    sort_partition(&partitions[0]);
    CHECK(partitions[0].num_keys == numKeys);

    for(size_t i = 0; i < numKeys; i++)
    {
        CHECK(strcmp(partitions[0].sorted[i]->key, expected[i]) == 0);
    }

    freePartitions();
}

// a key with one value returns it once, a key with many values returns each once, and both stay NULL after
static void testValuesAndNull(void)
{
    partition_t *p;
    char buffer[16];

    setupPartitions(1, 4);
    p = &partitions[0];

    addValue(p, "one", "x");

    for(int i = 0; i < 100; i++)
    {
        snprintf(buffer, sizeof(buffer), "%d", i);
        addValue(p, "many", buffer);
    }

    sort_partition(p);

    // walk the keys the same way the reducer will: set current, then drain every value
    for(p->current = 0; p->current < p->num_keys; p->current++)
    {
        char *key = p->sorted[p->current]->key;

        if(strcmp(key, "one") == 0)
        {
            char *value = get_next(key, 0);

            CHECK(value != NULL && strcmp(value, "x") == 0);
            CHECK(get_next(key, 0) == NULL);
            CHECK(get_next(key, 0) == NULL);
        }
        else
        {
            // keep track of which values we've seen so a duplicate or a missing one fails the test
            int seen[100] = { 0 };
            int count = 0;
            char *value;

            while((value = get_next(key, 0)) != NULL)
            {
                int number = atoi(value);

                CHECK(number >= 0 && number < 100 && !seen[number]);

                if(number >= 0 && number < 100)
                {
                    seen[number] = 1;
                }

                count++;
            }

            CHECK(count == 100);
            CHECK(get_next(key, 0) == NULL);
        }
    }

    freePartitions();
}

// asking for a key that isn't the one being reduced still works, and missing keys return NULL
static void testFallbackAndMissing(void)
{
    partition_t *p;
    char *value;
    char sameText[] = "beta";

    setupPartitions(1, 2);
    p = &partitions[0];

    addValue(p, "alpha", "1");
    addValue(p, "beta", "2");
    addValue(p, "gamma", "3");
    addValue(p, "", "empty");
    sort_partition(p);

    // current points at "", so asking for "gamma" has to use the bsearch path
    p->current = 0;
    value = get_next("gamma", 0);
    CHECK(value != NULL && strcmp(value, "3") == 0);
    CHECK(get_next("gamma", 0) == NULL);

    // same text as a stored key but a different pointer
    value = get_next(sameText, 0);
    CHECK(value != NULL && strcmp(value, "2") == 0);

    // the empty string is a valid key
    value = get_next("", 0);
    CHECK(value != NULL && strcmp(value, "empty") == 0);
    CHECK(get_next("", 0) == NULL);

    // a key that doesn't exist and partition numbers that are out of range
    CHECK(get_next("delta", 0) == NULL);
    CHECK(get_next("alpha", 5) == NULL);
    CHECK(get_next("alpha", -1) == NULL);

    freePartitions();
}

// a partition with zero keys shouldn't crash either function
static void testEmptyPartition(void)
{
    setupPartitions(2, 8);
    addValue(&partitions[1], "only", "v");

    sort_partition(&partitions[0]);
    sort_partition(&partitions[1]);

    CHECK(partitions[0].num_keys == 0);
    CHECK(partitions[0].sorted == NULL);
    CHECK(get_next("anything", 0) == NULL);

    partitions[1].current = 0;
    CHECK(strcmp(get_next("only", 1), "v") == 0);

    // right key, wrong partition
    CHECK(get_next("only", 0) == NULL);

    freePartitions();
}

// the same key in two partitions, read back and forth, should keep separate places in each list
static void testIndependentIteration(void)
{
    setupPartitions(2, 4);

    addValue(&partitions[0], "k", "p0-a");
    addValue(&partitions[0], "k", "p0-b");
    addValue(&partitions[1], "k", "p1-a");
    addValue(&partitions[1], "k", "p1-b");

    sort_partition(&partitions[0]);
    sort_partition(&partitions[1]);

    CHECK(strcmp(get_next("k", 0), "p0-a") == 0);
    CHECK(strcmp(get_next("k", 1), "p1-a") == 0);
    CHECK(strcmp(get_next("k", 0), "p0-b") == 0);
    CHECK(strcmp(get_next("k", 1), "p1-b") == 0);
    CHECK(get_next("k", 0) == NULL);
    CHECK(get_next("k", 1) == NULL);

    freePartitions();
}

int main(void)
{
    testSortedOrder();
    testValuesAndNull();
    testFallbackAndMissing();
    testEmptyPartition();
    testIndependentIteration();

    // the other members' static functions aren't called here, so this stops -Werror from complaining about them
    (void)store_init;
    (void)map_phase;
    (void)reduce_phase;
    (void)store_destroy;

    if(failures == 0)
    {
        printf("test_sort: all checks passed\n");
    }

    // return 0 if everything passed so scripts can tell the test succeeded
    return failures == 0 ? 0 : 1;
}
