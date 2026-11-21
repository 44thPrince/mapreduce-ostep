"""  Kevin Bernichio
    PA2 - Map Reduce (Member 3)
    COP4600 Operating Systems
    check_wordcount.py
"""

"""  - end-to-end correctness check for the whole MapReduce library
    - builds tests/wordcount against mapreduce.c with the required flags, makes a set of
    input files, works out the correct word counts on its own, then runs wordcount with
    different mapper and reducer counts and compares the output
    - only needs Python 3, nothing to install

    - run from anywhere:
        python3 tests/check_wordcount.py
        python3 tests/check_wordcount.py --runs 10     (repeat each run more times)
        python3 tests/check_wordcount.py --keep        (keep the generated input files)
"""

import argparse
import os
import random
import re
import shutil
import subprocess
import sys
import tempfile

# the exact flags the graders compile with
COMPILE_FLAGS = ["-Wall", "-Werror", "-pthread", "-O"]

# (mappers, reducers) pairs to try, from simple to stressful
CONFIGS = [
    (1, 1),     # no concurrency at all
    (4, 1),     # one partition, so the whole output must be in sorted order
    (10, 1),
    (1, 4),     # one mapper, several reducers
    (4, 4),
    (10, 10),   # same counts as the assignment's example
    (3, 7),     # counts that don't match up evenly
    (20, 3),    # more mappers than input files
    (16, 64),   # more reducers than most partitions need, so some partitions are empty
]

# a run that takes longer than this is treated as a hang or deadlock
TIMEOUT_SECONDS = 60


# finds the repo root, which is the folder above tests/
def findRepoRoot():
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# compiles wordcount.c and mapreduce.c into one program, returns the program's path
def buildWordcount(repoRoot, outputDir):
    """ - uses the CC environment variable if it's set, otherwise gcc
        - stops the whole script if it doesn't compile, since nothing else can run
    """

    # declare and initialize the compiler and the paths we'll need
    compiler = os.environ.get("CC", "gcc")
    program = os.path.join(outputDir, "wordcount")
    command = [compiler] + COMPILE_FLAGS + [
        "-I" + repoRoot,
        "-o", program,
        os.path.join(repoRoot, "tests", "wordcount.c"),
        os.path.join(repoRoot, "mapreduce.c"),
    ]

    print("Building: " + " ".join(command))
    result = subprocess.run(command, capture_output=True, text=True)

    # print the compiler errors so whoever broke the build can see them
    if result.returncode != 0:
        print(result.stderr)
        print("FAIL: mapreduce.c does not compile with " + " ".join(COMPILE_FLAGS))
        sys.exit(1)

    return program


# writes the input files the tests run on, returns two lists of file paths
def makeInputFiles(inputDir):
    """ - uses a fixed seed so every run of this script tests the same input
        - "main" is a mix of sizes, repeated words, mixed case, blank lines and extra spaces
        - "small" only has a handful of unique words, so with lots of reducers
        most partitions end up empty
    """

    # declare and initialize our random generator and the word pool we'll pick from
    rng = random.Random(4600)
    letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    wordPool = []

    for i in range(2000):
        wordPool.append("".join(rng.choice(letters) for j in range(rng.randint(1, 8))))

    # add some keys that are easy to get wrong when sorting
    wordPool += ["a", "ab", "abc", "A", "AB", "Z", "z", "0", "9", "aa", "aaa"]

    mainFiles = []
    smallFiles = []

    # several normal files of different sizes, using spaces and tabs between words
    for fileNumber in range(6):
        path = os.path.join(inputDir, "main_" + str(fileNumber) + ".txt")
        with open(path, "w") as f:
            for lineNumber in range(500 * (fileNumber + 1)):
                separator = rng.choice([" ", " ", " ", "\t", "  "])
                f.write(separator.join(rng.choice(wordPool) for k in range(rng.randint(0, 12))) + "\n")
        mainFiles.append(path)

    # one file with a single word repeated many times, so one key gets thousands of values
    path = os.path.join(inputDir, "main_repeated.txt")
    with open(path, "w") as f:
        for lineNumber in range(3000):
            f.write("repeat repeat repeat\n")
    mainFiles.append(path)

    # an empty file, and a file whose last line has no newline at the end
    path = os.path.join(inputDir, "main_empty.txt")
    open(path, "w").close()
    mainFiles.append(path)

    path = os.path.join(inputDir, "main_no_newline.txt")
    with open(path, "w") as f:
        f.write("last line without a newline")
    mainFiles.append(path)

    # small files that only use five unique words
    for fileNumber in range(3):
        path = os.path.join(inputDir, "small_" + str(fileNumber) + ".txt")
        with open(path, "w") as f:
            for lineNumber in range(50):
                f.write(" ".join(rng.choice(["cat", "dog", "Cat", "fish", "bird"]) for k in range(5)) + "\n")
        smallFiles.append(path)

    return mainFiles, smallFiles


# counts words the same way the assignment's Map does, without using our library at all
def expectedCounts(files):
    """ - Map reads with getline, which keeps the newline on each line, then splits on
    space, tab, newline and carriage return with strsep
        - strsep makes an empty token between two separators in a row, and at the end
    of every line that ends in a newline, so the empty string is a real key here
        - works on bytes so the sort order matches strcmp exactly
    """

    counts = {}

    for path in files:
        with open(path, "rb") as f:
            data = f.read()

        # rebuild the lines getline would return, each still ending in its newline
        pieces = data.split(b"\n")
        lines = [piece + b"\n" for piece in pieces[:-1]]

        if pieces[-1] != b"":
            lines.append(pieces[-1])

        # split each line the same way strsep does and count every token, even empty ones
        for line in lines:
            for token in re.split(rb"[ \t\n\r]", line):
                counts[token] = counts.get(token, 0) + 1

    return counts


# copy of MR_DefaultHashPartition so we know which partition each key should be in
def defaultHashPartition(key, numPartitions):
    hashValue = 5381

    # unsigned long is 64 bits on both macOS and Linux, so wrap around at 2^64 like C does
    for c in key:
        hashValue = (hashValue * 33 + c) % (2 ** 64)

    return hashValue % numPartitions


# runs wordcount once, returns a list of (key, count) pairs in the order they were printed
def runWordcount(program, files, mappers, reducers):
    """ - returns an error message instead of a list if the program crashes,
    hangs, or prints a line we can't read
    """

    # pass the mapper and reducer counts through the environment, like tests/wordcount.c expects
    environment = dict(os.environ, M=str(mappers), R=str(reducers))

    try:
        result = subprocess.run([program] + files, capture_output=True, env=environment, timeout=TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        return "timed out after " + str(TIMEOUT_SECONDS) + "s (possible deadlock)"

    # a negative return code means the program was killed by a signal, like a segfault
    if result.returncode < 0:
        return "crashed with signal " + str(-result.returncode)

    if result.returncode != 0:
        return "exited with code " + str(result.returncode)

    pairs = []

    for line in result.stdout.split(b"\n"):
        if line == b"":
            continue

        # split on the last space, since the key itself can be the empty string
        key, separator, countText = line.rpartition(b" ")

        if separator == b"" or not countText.isdigit():
            return "unreadable output line: " + repr(line)

        pairs.append((key, int(countText)))

    return pairs


# compares one run's output against the expected counts, returns a list of problems
def checkOutput(pairs, expected, reducers):
    """ - every key must show up exactly once, so Reduce was called once per key
        - every count must match, so the getter returned every value and then NULL
        - within each partition the keys must be in ascending order. Each partition
    is reduced by one thread, so its lines still come out in order even when other
    reducers print in between
        - with one reducer that means the whole output is sorted
    """

    # declare and initialize our list of problems and what we've seen so far
    problems = []
    seenCounts = {}
    lastKeyInPartition = {}

    for key, count in pairs:
        # the same key printed twice means Reduce was called more than once for it
        if key in seenCounts:
            problems.append("key " + repr(key) + " was reduced more than once")

        seenCounts[key] = count

        # check this key comes after the previous key printed for the same partition
        partition = defaultHashPartition(key, reducers)

        if partition in lastKeyInPartition and key < lastKeyInPartition[partition]:
            problems.append("partition " + str(partition) + " out of order: "
                            + repr(lastKeyInPartition[partition]) + " came before " + repr(key))

        lastKeyInPartition[partition] = key

    # keys we expected but never saw, and keys we saw but never emitted
    for key in expected:
        if key not in seenCounts:
            problems.append("missing key " + repr(key))

    for key in seenCounts:
        if key not in expected:
            problems.append("unexpected key " + repr(key))

    # counts that don't match
    for key in expected:
        if key in seenCounts and seenCounts[key] != expected[key]:
            problems.append("key " + repr(key) + ": expected " + str(expected[key])
                            + ", got " + str(seenCounts[key]))

    return problems


# runs every config on one input set, returns how many runs failed
def runTestSet(name, program, files, runs):
    expected = expectedCounts(files)
    failures = 0

    print("\n" + name + ": " + str(len(files)) + " files, " + str(len(expected)) + " unique keys, "
          + str(sum(expected.values())) + " total words")

    for mappers, reducers in CONFIGS:
        # declare and initialize the problems we find across every repeat of this config
        configProblems = []

        # thread bugs don't show up every time, so run each config several times
        for attempt in range(runs):
            output = runWordcount(program, files, mappers, reducers)

            if isinstance(output, str):
                configProblems.append("run " + str(attempt + 1) + ": " + output)
            else:
                for problem in checkOutput(output, expected, reducers):
                    configProblems.append("run " + str(attempt + 1) + ": " + problem)

        label = "  M=" + str(mappers).ljust(3) + "R=" + str(reducers).ljust(4)

        if len(configProblems) == 0:
            print(label + "PASS (" + str(runs) + " runs)")
        else:
            failures += 1
            print(label + "FAIL")

            # only show the first few problems so one bug doesn't flood the screen
            for problem in configProblems[:5]:
                print("      " + problem)

            if len(configProblems) > 5:
                print("      ... and " + str(len(configProblems) - 5) + " more")

    return failures


# with no input files, wordcount should exit cleanly and print nothing
def runNoFilesCheck(program):
    output = runWordcount(program, [], 4, 4)

    if isinstance(output, str):
        print("\nNo input files: FAIL (" + output + ")")
        return 1

    if len(output) != 0:
        print("\nNo input files: FAIL (printed " + str(len(output)) + " lines)")
        return 1

    print("\nNo input files: PASS")
    return 0


def main():
    parser = argparse.ArgumentParser(description="End-to-end correctness check for mapreduce.c")
    parser.add_argument("--runs", type=int, default=5, help="times to repeat each config (default 5)")
    parser.add_argument("--keep", action="store_true", help="keep the build and input files and print where they are")
    args = parser.parse_args()

    # declare and initialize the folders we'll build and write the input files into
    repoRoot = findRepoRoot()
    workDir = tempfile.mkdtemp(prefix="mr_check_")
    failures = 0

    try:
        program = buildWordcount(repoRoot, workDir)
        mainFiles, smallFiles = makeInputFiles(workDir)

        failures += runTestSet("Main input", program, mainFiles, args.runs)
        failures += runTestSet("Small input (mostly empty partitions)", program, smallFiles, args.runs)
        failures += runNoFilesCheck(program)
    finally:
        # clean up the temporary folder unless we were asked to keep it
        if args.keep:
            print("\nKept build and input files in " + workDir)
        else:
            shutil.rmtree(workDir, ignore_errors=True)

    if failures == 0:
        print("\nALL CHECKS PASSED")
    else:
        print("\n" + str(failures) + " CHECK(S) FAILED")

    # exit with 1 on failure so other scripts can tell something went wrong
    sys.exit(0 if failures == 0 else 1)


if __name__ == "__main__":
    main()
