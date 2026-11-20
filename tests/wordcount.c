/*  Wordcount test program from the PA2 assignment page.

    - Map and Reduce are copied exactly from the assignment
    - main reads the mapper and reducer counts from the M and R environment
    variables so check_wordcount.py can try different thread counts, and
    defaults to 10 and 10 like the original

    - build from the repo root:
        gcc -Wall -Werror -pthread -O -I. -o tests/wordcount tests/wordcount.c mapreduce.c
    - run:
        M=4 R=4 ./tests/wordcount file1.txt file2.txt
*/
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "mapreduce.h"

void Map(char *file_name) {
    FILE *fp = fopen(file_name, "r");
    assert(fp != NULL);

    char *line = NULL;
    size_t size = 0;
    while (getline(&line, &size, fp) != -1) {
        char *token, *dummy = line;
        while ((token = strsep(&dummy, " \t\n\r")) != NULL) {
            MR_Emit(token, "1");
        }
    }
    free(line);
    fclose(fp);
}

void Reduce(char *key, Getter get_next, int partition_number) {
    int count = 0;
    char *value;
    while ((value = get_next(key, partition_number)) != NULL)
        count++;
    printf("%s %d\n", key, count);
}

int main(int argc, char *argv[]) {
    int mappers = getenv("M") ? atoi(getenv("M")) : 10;
    int reducers = getenv("R") ? atoi(getenv("R")) : 10;
    MR_Run(argc, argv, Map, mappers, Reduce, reducers, MR_DefaultHashPartition);
    return 0;
}
