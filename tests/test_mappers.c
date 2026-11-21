#define _POSIX_C_SOURCE 200809L

#include "../mapreduce.c"

#include <errno.h>
#include <time.h>
#include <unistd.h>

#define MAX_TEST_FILES 100
#define TEST_FILE_PREFIX "/tmp/mapreduce_member2_test"

static pthread_mutex_t test_lock = PTHREAD_MUTEX_INITIALIZER;
static char expected_names[MAX_TEST_FILES][256];
static int seen[MAX_TEST_FILES];
static int expected_count;
static int active_maps;
static int max_active_maps;
static int test_failed;

static void fake_map(char *file_name) {
    int i;
    struct timespec delay;

    pthread_mutex_lock(&test_lock);
    active_maps++;
    if (active_maps > max_active_maps)
        max_active_maps = active_maps;

    for (i = 0; i < expected_count; i++) {
        if (strcmp(expected_names[i], file_name) == 0) {
            seen[i]++;
            break;
        }
    }
    if (i == expected_count)
        test_failed = 1;
    pthread_mutex_unlock(&test_lock);

    delay.tv_sec = 0;
    delay.tv_nsec = 100000000L;
    nanosleep(&delay, NULL);

    pthread_mutex_lock(&test_lock);
    active_maps--;
    pthread_mutex_unlock(&test_lock);
}

static int create_test_files(int count, char *argv[]) {
    int i;

    argv[0] = (char *)"test_mappers";
    expected_count = count;

    for (i = 0; i < count; i++) {
        FILE *file;
        int length;

        length = snprintf(expected_names[i], sizeof(expected_names[i]),
                          "%s_%ld_%d.txt", TEST_FILE_PREFIX,
                          (long)getpid(), i);
        if (length < 0 || (size_t)length >= sizeof(expected_names[i]))
            return -1;

        file = fopen(expected_names[i], "w");
        if (file == NULL)
            return -1;

        /* Give files different sizes so stat() and sorting are exercised. */
        if (i > 0) {
            int j;
            for (j = 0; j < i * 10; j++)
                fputc('x', file);
        }

        if (fclose(file) != 0)
            return -1;

        argv[i + 1] = expected_names[i];
    }

    return 0;
}

static void remove_test_files(int count) {
    int i;

    for (i = 0; i < count; i++)
        unlink(expected_names[i]);
}

static int run_case(int file_count, int mapper_count, int check_parallel) {
    char *argv[MAX_TEST_FILES + 1];
    int i;
    int result = 0;

    memset(seen, 0, sizeof(seen));
