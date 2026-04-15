#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <direct.h>
#define TEST_MKDIR(path) _mkdir(path)
#define TEST_RMDIR(path) _rmdir(path)
#else
#include <sys/stat.h>
#include <unistd.h>
#define TEST_MKDIR(path) mkdir(path, 0777)
#define TEST_RMDIR(path) rmdir(path)
#endif

#include "types.h"

#define BUFFER_SIZE 2048
#define LONG_NAME_SIZE 520

static int ensure_dir(const char *path);
static void remove_file_if_exists(const char *path);
static void remove_dir_if_exists(const char *path);
static int reset_test_environment(void);
static int prepare_dirs(void);
static int write_text_file(const char *path, const char *content);
static int read_text_file(const char *path, char *buffer, size_t size);

static int test_insert_values_success(void);
static int test_insert_named_columns_success(void);
static int test_insert_append_multiple_rows(void);
static int test_insert_missing_schema_fails(void);
static int test_insert_long_table_name_fails(void);
static int test_insert_schema_count_mismatch_fails(void);
static int test_insert_unknown_column_fails(void);
static int test_insert_duplicate_column_fails(void);
static int test_insert_null_column_entry_fails(void);
static int test_insert_escapes_csv_characters(void);
static int test_insert_auto_id_without_columns(void);
static int test_insert_auto_id_sequential(void);
static int test_insert_auto_id_with_explicit_id(void);

#define ASSERT_TRUE(expr)                                                        \
    do {                                                                         \
        if (!(expr)) {                                                           \
            fprintf(stderr, "Assertion failed at %s:%d: %s\n",                   \
                    __FILE__, __LINE__, #expr);                                  \
            goto cleanup;                                                        \
        }                                                                        \
    } while (0)

typedef int (*test_fn)(void);

typedef struct {
    const char *name;
    test_fn fn;
} TestCase;

int main(void)
{
    const TestCase tests[] = {
        {"insert values success", test_insert_values_success},
        {"insert named columns success", test_insert_named_columns_success},
        {"insert append multiple rows", test_insert_append_multiple_rows},
        {"insert missing schema fails", test_insert_missing_schema_fails},
        {"insert long table name fails", test_insert_long_table_name_fails},
        {"insert schema count mismatch fails", test_insert_schema_count_mismatch_fails},
        {"insert unknown column fails", test_insert_unknown_column_fails},
        {"insert duplicate column fails", test_insert_duplicate_column_fails},
        {"insert null column entry fails", test_insert_null_column_entry_fails},
        {"insert escapes csv characters", test_insert_escapes_csv_characters},
        {"insert auto-id without columns", test_insert_auto_id_without_columns},
        {"insert auto-id sequential", test_insert_auto_id_sequential},
        {"insert auto-id with explicit id", test_insert_auto_id_with_explicit_id},
    };
    size_t i;
    int failed = 0;

    for (i = 0; i < sizeof(tests) / sizeof(tests[0]); ++i) {
        int result = tests[i].fn();
        printf("[%s] %s\n", result == 0 ? "PASS" : "FAIL", tests[i].name);
        failed += result;
    }

    reset_test_environment();
    if (failed != 0) {
        fprintf(stderr, "%d test(s) failed.\n", failed);
        return 1;
    }

    printf("All INSERT storage tests passed.\n");
    return 0;
}

static int ensure_dir(const char *path)
{
    if (TEST_MKDIR(path) == 0 || errno == EEXIST) {
        return 0;
    }

    return -1;
}

static void remove_file_if_exists(const char *path)
{
    if (remove(path) != 0 && errno != ENOENT) {
        fprintf(stderr, "Failed to remove file: %s\n", path);
    }
}

static void remove_dir_if_exists(const char *path)
{
    if (TEST_RMDIR(path) != 0 && errno != ENOENT) {
        fprintf(stderr, "Failed to remove directory: %s\n", path);
    }
}

static int reset_test_environment(void)
{
    storage_reset_internal_caches();
    remove_file_if_exists("data/tables/events.csv");
    remove_file_if_exists("data/tables/quotes.csv");
    remove_file_if_exists("data/tables/users.csv");
    remove_file_if_exists("data/schema/events.schema");
    remove_file_if_exists("data/schema/quotes.schema");
    remove_file_if_exists("data/schema/users.schema");
    remove_dir_if_exists("data/tables");
    remove_dir_if_exists("data/schema");
    remove_dir_if_exists("data");
    return 0;
}

static int prepare_dirs(void)
{
    if (ensure_dir("data") != 0) {
        return -1;
    }

    if (ensure_dir("data/schema") != 0) {
        return -1;
    }

    if (ensure_dir("data/tables") != 0) {
        return -1;
    }

    return 0;
}

static int write_text_file(const char *path, const char *content)
{
    FILE *fp = fopen(path, "w");

    if (fp == NULL) {
        return -1;
    }

    if (content != NULL && fputs(content, fp) == EOF) {
        fclose(fp);
        return -1;
    }

    if (fclose(fp) != 0) {
        return -1;
    }

    return 0;
}

static int read_text_file(const char *path, char *buffer, size_t size)
{
    FILE *fp;
    size_t read_size;

    if (size == 0U) {
        return -1;
    }

    fp = fopen(path, "r");
    if (fp == NULL) {
        return -1;
    }

    read_size = fread(buffer, 1U, size - 1U, fp);
    if (ferror(fp)) {
        fclose(fp);
        return -1;
    }

    buffer[read_size] = '\0';
    fclose(fp);
    return 0;
}

static int test_insert_values_success(void)
{
    const char *values[] = {"1", "kim", "20"};
    char buffer[BUFFER_SIZE];
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_dirs() == 0);
    ASSERT_TRUE(write_text_file("data/schema/users.schema", "id,INT\nname,VARCHAR\nage,INT\n") == 0);
    ASSERT_TRUE(storage_insert("users", NULL, (char **)values, 3) == 0);
    ASSERT_TRUE(read_text_file("data/tables/users.csv", buffer, sizeof(buffer)) == 0);
    ASSERT_TRUE(strcmp(buffer, "1,kim,20\n") == 0);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_insert_named_columns_success(void)
{
    const char *columns[] = {"name", "age", "id"};
    const char *values[] = {"kim", "20", "1"};
    char buffer[BUFFER_SIZE];
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_dirs() == 0);
    ASSERT_TRUE(write_text_file("data/schema/users.schema", "id,INT\nname,VARCHAR\nage,INT\n") == 0);
    ASSERT_TRUE(storage_insert("users", (char **)columns, (char **)values, 3) == 0);
    ASSERT_TRUE(read_text_file("data/tables/users.csv", buffer, sizeof(buffer)) == 0);
    ASSERT_TRUE(strcmp(buffer, "1,kim,20\n") == 0);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_insert_append_multiple_rows(void)
{
    const char *first_values[] = {"1", "kim", "20"};
    const char *second_columns[] = {"age", "id", "name"};
    const char *second_values[] = {"31", "2", "lee"};
    char buffer[BUFFER_SIZE];
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_dirs() == 0);
    ASSERT_TRUE(write_text_file("data/schema/users.schema", "id,INT\nname,VARCHAR\nage,INT\n") == 0);
    ASSERT_TRUE(storage_insert("users", NULL, (char **)first_values, 3) == 0);
    ASSERT_TRUE(storage_insert("users", (char **)second_columns, (char **)second_values, 3) == 0);
    ASSERT_TRUE(read_text_file("data/tables/users.csv", buffer, sizeof(buffer)) == 0);
    ASSERT_TRUE(strcmp(buffer, "1,kim,20\n2,lee,31\n") == 0);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_insert_missing_schema_fails(void)
{
    const char *values[] = {"1", "kim", "20"};
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_dirs() == 0);
    ASSERT_TRUE(storage_insert("users", NULL, (char **)values, 3) == -1);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_insert_long_table_name_fails(void)
{
    const char *values[] = {"1", "kim", "20"};
    char table_name[LONG_NAME_SIZE];
    int i;
    int status = 1;

    for (i = 0; i < LONG_NAME_SIZE - 1; ++i) {
        table_name[i] = 'a';
    }
    table_name[LONG_NAME_SIZE - 1] = '\0';

    reset_test_environment();
    ASSERT_TRUE(prepare_dirs() == 0);
    ASSERT_TRUE(storage_insert(table_name, NULL, (char **)values, 3) == -1);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_insert_schema_count_mismatch_fails(void)
{
    const char *values[] = {"1", "kim"};
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_dirs() == 0);
    ASSERT_TRUE(write_text_file("data/schema/users.schema", "id,INT\nname,VARCHAR\nage,INT\n") == 0);
    ASSERT_TRUE(storage_insert("users", NULL, (char **)values, 2) == -1);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_insert_unknown_column_fails(void)
{
    const char *columns[] = {"id", "nickname", "age"};
    const char *values[] = {"1", "kim", "20"};
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_dirs() == 0);
    ASSERT_TRUE(write_text_file("data/schema/users.schema", "id,INT\nname,VARCHAR\nage,INT\n") == 0);
    ASSERT_TRUE(storage_insert("users", (char **)columns, (char **)values, 3) == -1);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_insert_duplicate_column_fails(void)
{
    const char *columns[] = {"id", "name", "name"};
    const char *values[] = {"1", "kim", "20"};
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_dirs() == 0);
    ASSERT_TRUE(write_text_file("data/schema/users.schema", "id,INT\nname,VARCHAR\nage,INT\n") == 0);
    ASSERT_TRUE(storage_insert("users", (char **)columns, (char **)values, 3) == -1);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_insert_null_column_entry_fails(void)
{
    const char *columns[] = {"id", NULL, "age"};
    const char *values[] = {"1", "kim", "20"};
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_dirs() == 0);
    ASSERT_TRUE(write_text_file("data/schema/users.schema", "id,INT\nname,VARCHAR\nage,INT\n") == 0);
    ASSERT_TRUE(storage_insert("users", (char **)columns, (char **)values, 3) == -1);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_insert_escapes_csv_characters(void)
{
    const char *values[] = {"1", "kim, \"minsu\"", "20"};
    char buffer[BUFFER_SIZE];
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_dirs() == 0);
    ASSERT_TRUE(write_text_file("data/schema/users.schema", "id,INT\nname,VARCHAR\nage,INT\n") == 0);
    ASSERT_TRUE(storage_insert("users", NULL, (char **)values, 3) == 0);
    ASSERT_TRUE(read_text_file("data/tables/users.csv", buffer, sizeof(buffer)) == 0);
    ASSERT_TRUE(strcmp(buffer, "1,\"kim, \"\"minsu\"\"\",20\n") == 0);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

/* ─── Week 7: auto-id 테스트 ─────────────────────────────────── */

static int test_insert_auto_id_without_columns(void)
{
    const char *columns[] = {"name", "age"};
    const char *values[] = {"alice", "25"};
    char buffer[BUFFER_SIZE];
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_dirs() == 0);
    ASSERT_TRUE(write_text_file("data/schema/users.schema", "id,INT\nname,VARCHAR\nage,INT\n") == 0);
    ASSERT_TRUE(storage_insert("users", (char **)columns, (char **)values, 2) == 0);
    ASSERT_TRUE(read_text_file("data/tables/users.csv", buffer, sizeof(buffer)) == 0);
    ASSERT_TRUE(strcmp(buffer, "1,alice,25\n") == 0);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_insert_auto_id_sequential(void)
{
    const char *columns[] = {"name", "age"};
    const char *values1[] = {"alice", "25"};
    const char *values2[] = {"bob", "30"};
    char buffer[BUFFER_SIZE];
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_dirs() == 0);
    ASSERT_TRUE(write_text_file("data/schema/users.schema", "id,INT\nname,VARCHAR\nage,INT\n") == 0);
    ASSERT_TRUE(storage_insert("users", (char **)columns, (char **)values1, 2) == 0);
    ASSERT_TRUE(storage_insert("users", (char **)columns, (char **)values2, 2) == 0);
    ASSERT_TRUE(read_text_file("data/tables/users.csv", buffer, sizeof(buffer)) == 0);
    ASSERT_TRUE(strcmp(buffer, "1,alice,25\n2,bob,30\n") == 0);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_insert_auto_id_with_explicit_id(void)
{
    const char *columns[] = {"id", "name", "age"};
    const char *values[] = {"99", "charlie", "35"};
    char buffer[BUFFER_SIZE];
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_dirs() == 0);
    ASSERT_TRUE(write_text_file("data/schema/users.schema", "id,INT\nname,VARCHAR\nage,INT\n") == 0);
    ASSERT_TRUE(storage_insert("users", (char **)columns, (char **)values, 3) == 0);
    ASSERT_TRUE(read_text_file("data/tables/users.csv", buffer, sizeof(buffer)) == 0);
    ASSERT_TRUE(strcmp(buffer, "99,charlie,35\n") == 0);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}
