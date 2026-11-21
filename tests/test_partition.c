/*  Diego Mendoza
    PA2 - Map Reduce (Member 4)
    COP4600 Operating Systems
    test_partition.c
*/

/* Tests partition routing, key order, and cleanup with unread values. */
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include "mapreduce.h"

static int visited[3];

static unsigned long route_key(char *key, int count)
{
    assert(count == 3);
    return key[0] == 'z' ? 1 : 0;
}

static void map_fixture(char *file)
{
    if (strcmp(file, "first") == 0) {
        MR_Emit("banana", "1");
        MR_Emit("apple", "1");
        MR_Emit("zebra", "1");
    } else {
        assert(strcmp(file, "second") == 0);
        MR_Emit("zebra", "1");
        MR_Emit("apple", "1");
        MR_Emit("banana", "1");
    }
}

static void check_value(char *value)
{
    assert(value != NULL && strcmp(value, "1") == 0);
}

static void reduce_fixture(char *key, Getter next, int number)
{
    assert(route_key(key, 3) == (unsigned long)number);
    if (number == 0 && visited[0] == 0) {
        assert(strcmp(key, "apple") == 0);
        check_value(next(key, number));
    } else if (number == 0) {
        assert(visited[0] == 1 && strcmp(key, "banana") == 0);
    } else {
        assert(number == 1 && visited[1] == 0 && strcmp(key, "zebra") == 0);
        check_value(next(key, number));
        check_value(next(key, number));
        char *end = next(key, number);
        assert(end == NULL);
    }
    ++visited[number];
}

int main(void)
{
    char *argv[] = { "test_partition", "first", "second", NULL };
    for (int i = 0; i < 2; ++i) {
        memset(visited, 0, sizeof(visited));
        MR_Run(3, argv, map_fixture, 2, reduce_fixture, 3, route_key);
        assert(visited[0] == 2 && visited[1] == 1 && visited[2] == 0);
    }

    char *empty[] = { argv[0], NULL };
    memset(visited, 0, sizeof(visited));
    MR_Run(1, empty, map_fixture, 2, reduce_fixture, 3, route_key);
    assert(visited[0] == 0 && visited[1] == 0 && visited[2] == 0);
    puts("Member 4 lifecycle and reducer checks passed");
}
