#include "types.h"
#include "bptree.h"
#include "index_registry.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <direct.h>
#include <io.h>
#define dup _dup
#define dup2 _dup2
#define close _close
#define fileno _fileno
#define make_dir(path) _mkdir(path)
#else
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#define make_dir(path) mkdir(path, 0775)
#endif

#define DATA_DIR "data"
#define DATA_SCHEMA_DIR DATA_DIR "/schema"
#define DATA_TABLES_DIR DATA_DIR "/tables"
#define TEST_OUTPUT_PATH DATA_DIR "/test_output.txt"
#define USERS_SCHEMA_PATH DATA_DIR "/users.schema"
#define USERS_CSV_PATH DATA_DIR "/users.csv"
#define USERS_NESTED_SCHEMA_PATH DATA_SCHEMA_DIR "/users.schema"
#define USERS_NESTED_CSV_PATH DATA_TABLES_DIR "/users.csv"

static int seed_users_index(void)
{
    BPTree *tree;

    index_registry_destroy_all();
    tree = index_registry_get_or_create("users", 8);
    if (tree == NULL) {
        return 1;
    }

    bptree_insert(tree, 1, 0);
    bptree_insert(tree, 2, 1);
    bptree_insert(tree, 3, 2);
    bptree_insert(tree, 4, 3);
    return 0;
}

static char *duplicate_string(const char *source)
{
    size_t length;
    char *copy;

    length = strlen(source);
    copy = (char *)malloc(length + 1U);
    if (copy == NULL) {
        return NULL;
    }

    memcpy(copy, source, length + 1U);
    return copy;
}

static void fail_test(const char *message)
{
    fprintf(stderr, "[EXECUTOR] FAIL: %s\n", message);
}

/* Create the local data directory so fixtures work in a clean checkout. */
static int ensure_data_dir(void)
{
    if (make_dir(DATA_DIR) == 0) {
        return 0;
    }

    if (errno == EEXIST) {
        return 0;
    }

    return 1;
}

static int ensure_executor_fixture_dirs(void)
{
    if (ensure_data_dir() != 0) {
        return 1;
    }

    if (make_dir(DATA_SCHEMA_DIR) != 0 && errno != EEXIST) {
        return 1;
    }

    if (make_dir(DATA_TABLES_DIR) != 0 && errno != EEXIST) {
        return 1;
    }

    return 0;
}

static char *read_text_file(const char *path)
{
    FILE *file;
    long size;
    size_t read_size;
    char *buffer;

    file = fopen(path, "rb");
    if (file == NULL) {
        return NULL;
    }

    if (fseek(file, 0L, SEEK_END) != 0) {
        fclose(file);
        return NULL;
    }

    size = ftell(file);
    if (size < 0) {
        fclose(file);
        return NULL;
    }

    if (fseek(file, 0L, SEEK_SET) != 0) {
        fclose(file);
        return NULL;
    }

    buffer = (char *)malloc((size_t)size + 1U);
    if (buffer == NULL) {
        fclose(file);
        return NULL;
    }

    read_size = fread(buffer, 1U, (size_t)size, file);
    fclose(file);

    if (read_size != (size_t)size) {
        free(buffer);
        return NULL;
    }

    buffer[size] = '\0';
    return buffer;
}

static int write_text_file(const char *path, const char *content)
{
    FILE *file;

    file = fopen(path, "w");
    if (file == NULL) {
        return 1;
    }

    if (fputs(content, file) == EOF) {
        fclose(file);
        return 1;
    }

    fclose(file);
    return 0;
}

static void cleanup_fixture_files(void)
{
    remove(TEST_OUTPUT_PATH);
    remove(USERS_CSV_PATH);
    remove(USERS_SCHEMA_PATH);
    remove(USERS_NESTED_CSV_PATH);
    remove(USERS_NESTED_SCHEMA_PATH);
}

/* Recreate fixture files so executor tests still pass after make clean. */
static int prepare_users_fixture(void)
{
    static const char *schema =
        "id INT\n"
        "name VARCHAR\n"
        "age INT\n"
        "city VARCHAR\n"
        "active BOOLEAN\n"
        "joined DATE\n"
        "score FLOAT\n";
    static const char *csv =
        "1,Alice,29,Seoul,true,2024-01-12,88.5\n"
        "2,Bob,24,Busan,false,2023-11-03,91.0\n"
        "3,Chloe,27,Seoul,true,2024-02-28,79.2\n"
        "4,Dylan,31,Incheon,true,2022-08-19,95.1\n";

    if (ensure_executor_fixture_dirs() != 0) {
        return 1;
    }

    if (write_text_file(USERS_SCHEMA_PATH, schema) != 0) {
        return 1;
    }

    if (write_text_file(USERS_CSV_PATH, csv) != 0) {
        return 1;
    }

    if (write_text_file(USERS_NESTED_SCHEMA_PATH, schema) != 0) {
        return 1;
    }

    if (write_text_file(USERS_NESTED_CSV_PATH, csv) != 0) {
        return 1;
    }

    return 0;
}

/* Redirect stdout so we can assert on the exact SELECT output. */
static int capture_stdout_for_select(ParsedSQL *sql, char **output)
{
    int saved_stdout;
    FILE *redirected;

    fflush(stdout);
    saved_stdout = dup(fileno(stdout));
    if (saved_stdout < 0) {
        return 1;
    }

    redirected = freopen(TEST_OUTPUT_PATH, "w", stdout);
    if (redirected == NULL) {
        close(saved_stdout);
        return 1;
    }

    execute(sql);
    fflush(stdout);

    if (dup2(saved_stdout, fileno(stdout)) < 0) {
        close(saved_stdout);
        return 1;
    }
    close(saved_stdout);

    *output = read_text_file(TEST_OUTPUT_PATH);
    remove(TEST_OUTPUT_PATH);
    return (*output == NULL) ? 1 : 0;
}

static ParsedSQL *make_base_select(void)
{
    ParsedSQL *sql;

    sql = (ParsedSQL *)calloc(1U, sizeof(ParsedSQL));
    if (sql == NULL) {
        return NULL;
    }

    sql->type = QUERY_SELECT;
    strcpy(sql->table, "users");
    sql->limit = -1;
    return sql;
}

static int contains_text(const char *haystack, const char *needle)
{
    return haystack != NULL && needle != NULL && strstr(haystack, needle) != NULL;
}

static int test_execute_select_with_where_order_limit(void)
{
    ParsedSQL *sql;
    char *output;

    sql = make_base_select();
    if (sql == NULL) {
        fail_test("Failed to allocate ParsedSQL.");
        return 1;
    }

    sql->col_count = 2;
    sql->columns = (char **)calloc(2U, sizeof(char *));
    sql->columns[0] = duplicate_string("name");
    sql->columns[1] = duplicate_string("age");
    sql->where_count = 2;
    sql->where = (WhereClause *)calloc(2U, sizeof(WhereClause));
    strcpy(sql->where_logic, "AND");
    strcpy(sql->where[0].column, "age");
    strcpy(sql->where[0].op, ">");
    strcpy(sql->where[0].value, "25");
    strcpy(sql->where[1].column, "active");
    strcpy(sql->where[1].op, "=");
    strcpy(sql->where[1].value, "true");
    sql->order_by = (OrderBy *)calloc(1U, sizeof(OrderBy));
    strcpy(sql->order_by->column, "age");
    sql->order_by->asc = 0;
    sql->limit = 2;

    if (capture_stdout_for_select(sql, &output) != 0) {
        free_parsed(sql);
        fail_test("Failed to capture SELECT output.");
        return 1;
    }

    if (!contains_text(output, "name | age") ||
        !contains_text(output, "Dylan | 31") ||
        !contains_text(output, "Alice | 29") ||
        !contains_text(output, "(2 rows)")) {
        free(output);
        free_parsed(sql);
        fail_test("SELECT output did not include the filtered and sorted rows.");
        return 1;
    }

    free(output);
    free_parsed(sql);
    return 0;
}

static int test_storage_select_count_star(void)
{
    ParsedSQL *sql;
    char *output;

    sql = make_base_select();
    if (sql == NULL) {
        fail_test("Failed to allocate ParsedSQL.");
        return 1;
    }

    sql->col_count = 1;
    sql->columns = (char **)calloc(1U, sizeof(char *));
    sql->columns[0] = duplicate_string("COUNT(*)");
    sql->where_count = 2;
    sql->where = (WhereClause *)calloc(2U, sizeof(WhereClause));
    strcpy(sql->where_logic, "OR");
    strcpy(sql->where[0].column, "city");
    strcpy(sql->where[0].op, "=");
    strcpy(sql->where[0].value, "'Seoul'");
    strcpy(sql->where[1].column, "active");
    strcpy(sql->where[1].op, "=");
    strcpy(sql->where[1].value, "false");

    if (capture_stdout_for_select(sql, &output) != 0) {
        free_parsed(sql);
        fail_test("Failed to capture COUNT(*) output.");
        return 1;
    }

    if (!contains_text(output, "COUNT(*)") || !contains_text(output, "\n3\n")) {
        free(output);
        free_parsed(sql);
        fail_test("COUNT(*) output was not correct.");
        return 1;
    }

    free(output);
    free_parsed(sql);
    return 0;
}

static int test_storage_select_rejects_unknown_column(void)
{
    ParsedSQL *sql;
    int status;

    sql = make_base_select();
    if (sql == NULL) {
        fail_test("Failed to allocate ParsedSQL.");
        return 1;
    }

    sql->col_count = 1;
    sql->columns = (char **)calloc(1U, sizeof(char *));
    sql->columns[0] = duplicate_string("missing_column");

    status = storage_select(sql->table, sql);
    free_parsed(sql);

    if (status == 0) {
        fail_test("Unknown column should have failed.");
        return 1;
    }

    return 0;
}

static int test_execute_select_where_id_uses_bptree_path(void)
{
    ParsedSQL *sql;
    char *output;

    if (seed_users_index() != 0) {
        fail_test("Failed to seed users index.");
        return 1;
    }

    sql = make_base_select();
    if (sql == NULL) {
        fail_test("Failed to allocate ParsedSQL.");
        return 1;
    }

    sql->col_count = 2;
    sql->columns = (char **)calloc(2U, sizeof(char *));
    sql->columns[0] = duplicate_string("name");
    sql->columns[1] = duplicate_string("age");
    sql->where_count = 1;
    sql->where = (WhereClause *)calloc(1U, sizeof(WhereClause));
    strcpy(sql->where[0].column, "id");
    strcpy(sql->where[0].op, "=");
    strcpy(sql->where[0].value, "3");

    if (capture_stdout_for_select(sql, &output) != 0) {
        free_parsed(sql);
        fail_test("Failed to capture indexed SELECT output.");
        return 1;
    }

    if (!contains_text(output, "name | age") ||
        !contains_text(output, "Chloe | 27") ||
        !contains_text(output, "(1 rows)")) {
        free(output);
        free_parsed(sql);
        fail_test("Indexed SELECT output did not include the indexed row.");
        return 1;
    }

    free(output);
    free_parsed(sql);
    return 0;
}

static int test_execute_select_where_id_count_star(void)
{
    ParsedSQL *sql;
    char *output;

    if (seed_users_index() != 0) {
        fail_test("Failed to seed users index.");
        return 1;
    }

    sql = make_base_select();
    if (sql == NULL) {
        fail_test("Failed to allocate ParsedSQL.");
        return 1;
    }

    sql->col_count = 1;
    sql->columns = (char **)calloc(1U, sizeof(char *));
    sql->columns[0] = duplicate_string("COUNT(*)");
    sql->where_count = 1;
    sql->where = (WhereClause *)calloc(1U, sizeof(WhereClause));
    strcpy(sql->where[0].column, "id");
    strcpy(sql->where[0].op, "=");
    strcpy(sql->where[0].value, "3");

    if (capture_stdout_for_select(sql, &output) != 0) {
        free_parsed(sql);
        fail_test("Failed to capture indexed COUNT(*) output.");
        return 1;
    }

    if (!contains_text(output, "COUNT(*)") ||
        !contains_text(output, "\n1\n") ||
        !contains_text(output, "(1 rows)")) {
        free(output);
        free_parsed(sql);
        fail_test("Indexed COUNT(*) output was not correct.");
        return 1;
    }

    free(output);
    free_parsed(sql);
    return 0;
}

static int test_execute_select_where_id_without_registry_falls_back(void)
{
    ParsedSQL *sql;
    char *output;

    index_registry_destroy_all();

    sql = make_base_select();
    if (sql == NULL) {
        fail_test("Failed to allocate ParsedSQL.");
        return 1;
    }

    sql->col_count = 2;
    sql->columns = (char **)calloc(2U, sizeof(char *));
    sql->columns[0] = duplicate_string("name");
    sql->columns[1] = duplicate_string("age");
    sql->where_count = 1;
    sql->where = (WhereClause *)calloc(1U, sizeof(WhereClause));
    strcpy(sql->where[0].column, "id");
    strcpy(sql->where[0].op, "=");
    strcpy(sql->where[0].value, "3");

    if (capture_stdout_for_select(sql, &output) != 0) {
        free_parsed(sql);
        fail_test("Failed to capture fallback SELECT output.");
        return 1;
    }

    if (!contains_text(output, "name | age") ||
        !contains_text(output, "Chloe | 27") ||
        !contains_text(output, "(1 rows)")) {
        free(output);
        free_parsed(sql);
        fail_test("WHERE id query should fall back to storage_select when registry is missing.");
        return 1;
    }

    free(output);
    free_parsed(sql);
    return 0;
}

static int test_execute_select_non_id_where_keeps_storage_path(void)
{
    ParsedSQL *sql;
    char *output;

    if (seed_users_index() != 0) {
        fail_test("Failed to seed users index.");
        return 1;
    }

    sql = make_base_select();
    if (sql == NULL) {
        fail_test("Failed to allocate ParsedSQL.");
        return 1;
    }

    sql->col_count = 2;
    sql->columns = (char **)calloc(2U, sizeof(char *));
    sql->columns[0] = duplicate_string("name");
    sql->columns[1] = duplicate_string("age");
    sql->where_count = 1;
    sql->where = (WhereClause *)calloc(1U, sizeof(WhereClause));
    strcpy(sql->where[0].column, "name");
    strcpy(sql->where[0].op, "=");
    strcpy(sql->where[0].value, "'Bob'");

    if (capture_stdout_for_select(sql, &output) != 0) {
        free_parsed(sql);
        fail_test("Failed to capture non-indexed SELECT output.");
        return 1;
    }

    if (!contains_text(output, "name | age") ||
        !contains_text(output, "Bob | 24") ||
        !contains_text(output, "(1 rows)")) {
        free(output);
        free_parsed(sql);
        fail_test("Non-id WHERE query should keep storage_select behavior.");
        return 1;
    }

    free(output);
    free_parsed(sql);
    return 0;
}

static int test_execute_select_where_id_missing_prints_empty_result(void)
{
    ParsedSQL *sql;
    char *output;

    if (seed_users_index() != 0) {
        fail_test("Failed to seed users index.");
        return 1;
    }

    sql = make_base_select();
    if (sql == NULL) {
        fail_test("Failed to allocate ParsedSQL.");
        return 1;
    }

    sql->col_count = 2;
    sql->columns = (char **)calloc(2U, sizeof(char *));
    sql->columns[0] = duplicate_string("name");
    sql->columns[1] = duplicate_string("age");
    sql->where_count = 1;
    sql->where = (WhereClause *)calloc(1U, sizeof(WhereClause));
    strcpy(sql->where[0].column, "id");
    strcpy(sql->where[0].op, "=");
    strcpy(sql->where[0].value, "999");

    if (capture_stdout_for_select(sql, &output) != 0) {
        free_parsed(sql);
        fail_test("Failed to capture empty indexed SELECT output.");
        return 1;
    }

    if (!contains_text(output, "name | age") ||
        !contains_text(output, "(0 rows)")) {
        free(output);
        free_parsed(sql);
        fail_test("Missing id query did not preserve empty-result formatting.");
        return 1;
    }

    free(output);
    free_parsed(sql);
    return 0;
}

static int test_execute_select_where_id_between_uses_range_path(void)
{
    ParsedSQL *sql;
    char *output;

    if (seed_users_index() != 0) {
        fail_test("Failed to seed users index.");
        return 1;
    }

    sql = make_base_select();
    if (sql == NULL) {
        fail_test("Failed to allocate ParsedSQL.");
        return 1;
    }

    sql->col_count = 2;
    sql->columns = (char **)calloc(2U, sizeof(char *));
    sql->columns[0] = duplicate_string("name");
    sql->columns[1] = duplicate_string("age");
    sql->where_count = 1;
    sql->where = (WhereClause *)calloc(1U, sizeof(WhereClause));
    strcpy(sql->where[0].column, "id");
    strcpy(sql->where[0].op, "BETWEEN");
    strcpy(sql->where[0].value, "2");
    strcpy(sql->where[0].value_to, "3");

    if (capture_stdout_for_select(sql, &output) != 0) {
        free_parsed(sql);
        fail_test("Failed to capture BETWEEN range SELECT output.");
        return 1;
    }

    if (!contains_text(output, "name | age") ||
        !contains_text(output, "Bob | 24") ||
        !contains_text(output, "Chloe | 27") ||
        !contains_text(output, "(2 rows)")) {
        free(output);
        free_parsed(sql);
        fail_test("BETWEEN range query did not return the expected rows.");
        return 1;
    }

    free(output);
    free_parsed(sql);
    return 0;
}

static int test_execute_select_where_id_between_includes_boundaries(void)
{
    ParsedSQL *sql;
    char *output;

    if (seed_users_index() != 0) {
        fail_test("Failed to seed users index.");
        return 1;
    }

    sql = make_base_select();
    if (sql == NULL) {
        fail_test("Failed to allocate ParsedSQL.");
        return 1;
    }

    sql->col_count = 2;
    sql->columns = (char **)calloc(2U, sizeof(char *));
    sql->columns[0] = duplicate_string("name");
    sql->columns[1] = duplicate_string("age");
    sql->where_count = 1;
    sql->where = (WhereClause *)calloc(1U, sizeof(WhereClause));
    strcpy(sql->where[0].column, "id");
    strcpy(sql->where[0].op, "BETWEEN");
    strcpy(sql->where[0].value, "1");
    strcpy(sql->where[0].value_to, "4");

    if (capture_stdout_for_select(sql, &output) != 0) {
        free_parsed(sql);
        fail_test("Failed to capture BETWEEN boundary SELECT output.");
        return 1;
    }

    if (!contains_text(output, "name | age") ||
        !contains_text(output, "Alice | 29") ||
        !contains_text(output, "Dylan | 31") ||
        !contains_text(output, "(4 rows)")) {
        free(output);
        free_parsed(sql);
        fail_test("BETWEEN boundary query did not include both endpoints.");
        return 1;
    }

    free(output);
    free_parsed(sql);
    return 0;
}

static int test_execute_select_where_id_between_missing_prints_empty_result(void)
{
    ParsedSQL *sql;
    char *output;

    if (seed_users_index() != 0) {
        fail_test("Failed to seed users index.");
        return 1;
    }

    sql = make_base_select();
    if (sql == NULL) {
        fail_test("Failed to allocate ParsedSQL.");
        return 1;
    }

    sql->col_count = 2;
    sql->columns = (char **)calloc(2U, sizeof(char *));
    sql->columns[0] = duplicate_string("name");
    sql->columns[1] = duplicate_string("age");
    sql->where_count = 1;
    sql->where = (WhereClause *)calloc(1U, sizeof(WhereClause));
    strcpy(sql->where[0].column, "id");
    strcpy(sql->where[0].op, "BETWEEN");
    strcpy(sql->where[0].value, "100");
    strcpy(sql->where[0].value_to, "120");

    if (capture_stdout_for_select(sql, &output) != 0) {
        free_parsed(sql);
        fail_test("Failed to capture empty BETWEEN SELECT output.");
        return 1;
    }

    if (!contains_text(output, "name | age") ||
        !contains_text(output, "(0 rows)")) {
        free(output);
        free_parsed(sql);
        fail_test("Missing BETWEEN query did not preserve empty-result formatting.");
        return 1;
    }

    free(output);
    free_parsed(sql);
    return 0;
}

int run_executor_tests(void)
{
    int status;

    fprintf(stderr, "[EXECUTOR TESTS]\n");
    status = 0;
    cleanup_fixture_files();

    if (prepare_users_fixture() != 0) {
        fail_test("Failed to prepare fixture files.");
        status = 1;
        goto cleanup;
    }

    if (test_execute_select_with_where_order_limit() != 0) {
        status = 1;
        goto cleanup;
    }

    if (test_storage_select_count_star() != 0) {
        status = 1;
        goto cleanup;
    }

    if (test_storage_select_rejects_unknown_column() != 0) {
        status = 1;
        goto cleanup;
    }

    if (test_execute_select_where_id_uses_bptree_path() != 0) {
        status = 1;
        goto cleanup;
    }

    if (test_execute_select_where_id_count_star() != 0) {
        status = 1;
        goto cleanup;
    }

    if (test_execute_select_where_id_without_registry_falls_back() != 0) {
        status = 1;
        goto cleanup;
    }

    if (test_execute_select_non_id_where_keeps_storage_path() != 0) {
        status = 1;
        goto cleanup;
    }

    if (test_execute_select_where_id_missing_prints_empty_result() != 0) {
        status = 1;
        goto cleanup;
    }

    if (test_execute_select_where_id_between_uses_range_path() != 0) {
        status = 1;
        goto cleanup;
    }

    if (test_execute_select_where_id_between_includes_boundaries() != 0) {
        status = 1;
        goto cleanup;
    }

    if (test_execute_select_where_id_between_missing_prints_empty_result() != 0) {
        status = 1;
        goto cleanup;
    }

cleanup:
    cleanup_fixture_files();
    index_registry_destroy_all();
    if (status == 0) {
        fprintf(stderr, "[EXECUTOR TESTS] passed\n");
    }
    return status;
}
