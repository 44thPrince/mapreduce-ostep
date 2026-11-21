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
    expected_count = 0;
    active_maps = 0;
    max_active_maps = 0;
    test_failed = 0;

    if (create_test_files(file_count, argv) != 0) {
        fprintf(stderr, "Could not create test files\n");
        remove_test_files(file_count);
        return 1;
    }

    map_fn = fake_map;
    map_phase(file_count + 1, argv, mapper_count);

    pthread_mutex_lock(&test_lock);
    if (test_failed)
        result = 1;

    for (i = 0; i < file_count; i++) {
        if (seen[i] != 1) {
            fprintf(stderr, "File %s seen %d times (expected 1)\n",
                    expected_names[i], seen[i]);
            result = 1;
        }
    }

    if (check_parallel && max_active_maps < 2) {
        fprintf(stderr, "Map calls did not overlap; max active = %d\n",
                max_active_maps);
        result = 1;
    }
    pthread_mutex_unlock(&test_lock);

    remove_test_files(file_count);
    return result;
}

int main(void) {
    int failures = 0;

    if (run_case(0, 4, 0) != 0) {
        fprintf(stderr, "FAIL: zero files\n");
        failures++;
    } else {
        printf("PASS: zero files\n");
    }

    if (run_case(1, 1, 0) != 0) {
        fprintf(stderr, "FAIL: one file\n");
        failures++;
    } else {
        printf("PASS: one file\n");
    }

    if (run_case(3, 10, 0) != 0) {
        fprintf(stderr, "FAIL: 3 files, 10 mappers\n");
        failures++;
    } else {
        printf("PASS: 3 files, 10 mappers\n");
    }

    if (run_case(50, 4, 0) != 0) {
        fprintf(stderr, "FAIL: 50 files, 4 mappers\n");
        failures++;
    } else {
        printf("PASS: 50 files, 4 mappers\n");
    }

    if (run_case(8, 4, 1) != 0) {
        fprintf(stderr, "FAIL: parallel execution\n");
        failures++;
    } else {
        printf("PASS: parallel execution\n");
    }

    pthread_mutex_destroy(&test_lock);

    if (failures != 0) {
        fprintf(stderr, "%d test(s) failed\n", failures);
        return EXIT_FAILURE;
    }

    printf("All mapper tests passed\n");
    return EXIT_SUCCESS;
}
