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
#include "bptree.h"
#include "index_registry.h"

#define BUFFER_SIZE 4096
#define PATH_SIZE 512

static int ensure_dir(const char *path);
static void remove_file_if_exists(const char *path);
static void remove_dir_if_exists(const char *path);
static int reset_test_environment(void);
static int prepare_dirs(void);
static int build_schema_path(const char *table, char *buffer, size_t size);
static int build_table_path(const char *table, char *buffer, size_t size);
static int write_text_file(const char *path, const char *content);
static int read_text_file(const char *path, char *buffer, size_t size);
static int prepare_table(const char *table, const char *schema_content, const char *table_content);
static WhereClause make_where(const char *column, const char *op, const char *value);
static int call_storage_delete(const char *table, WhereClause *where, int where_count,
                               char **where_links, const char *where_logic);

static int test_delete_all_rows_success(void);
static int test_delete_single_equals_success(void);
static int test_delete_int_comparison_success(void);
static int test_delete_float_comparison_success(void);
static int test_delete_boolean_comparison_success(void);
static int test_delete_date_comparison_success(void);
static int test_delete_like_success(void);
static int test_delete_quoted_field_success(void);
static int test_delete_multiline_field_success(void);
static int test_delete_zero_match_success(void);
static int test_delete_missing_schema_fails(void);
static int test_delete_missing_table_fails(void);
static int test_delete_multiple_where_and_success(void);
static int test_delete_multiple_where_or_success(void);
static int test_delete_multiple_where_mixed_success(void);
static int test_delete_multiple_where_fallback_success(void);
static int test_delete_unknown_column_fails(void);
static int test_delete_invalid_operator_fails(void);
static int test_delete_malformed_csv_fails(void);
static int test_delete_row_count_mismatch_fails(void);
static int test_delete_datetime_non_equality_fails(void);
static int test_delete_index_sync_deleted_id_gone(void);
static int test_delete_index_sync_remaining_row_idx(void);

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
        {"delete all rows success", test_delete_all_rows_success},
        {"delete single equals success", test_delete_single_equals_success},
        {"delete int comparison success", test_delete_int_comparison_success},
        {"delete float comparison success", test_delete_float_comparison_success},
        {"delete boolean comparison success", test_delete_boolean_comparison_success},
        {"delete date comparison success", test_delete_date_comparison_success},
        {"delete like success", test_delete_like_success},
        {"delete quoted field success", test_delete_quoted_field_success},
        {"delete multiline field success", test_delete_multiline_field_success},
        {"delete zero match success", test_delete_zero_match_success},
        {"delete missing schema fails", test_delete_missing_schema_fails},
        {"delete missing table fails", test_delete_missing_table_fails},
        {"delete multiple where AND success", test_delete_multiple_where_and_success},
        {"delete multiple where OR success", test_delete_multiple_where_or_success},
        {"delete multiple where mixed success", test_delete_multiple_where_mixed_success},
        {"delete multiple where fallback success", test_delete_multiple_where_fallback_success},
        {"delete unknown column fails", test_delete_unknown_column_fails},
        {"delete invalid operator fails", test_delete_invalid_operator_fails},
        {"delete malformed csv fails", test_delete_malformed_csv_fails},
        {"delete row count mismatch fails", test_delete_row_count_mismatch_fails},
        {"delete datetime non equality fails", test_delete_datetime_non_equality_fails},
        {"delete index sync: deleted id gone from tree", test_delete_index_sync_deleted_id_gone},
        {"delete index sync: remaining row_idx correct", test_delete_index_sync_remaining_row_idx},
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

    printf("All DELETE storage tests passed.\n");
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
    remove_file_if_exists("data/tables/broken.csv");
    remove_file_if_exists("data/tables/dates.csv");
    remove_file_if_exists("data/tables/datetimes.csv");
    remove_file_if_exists("data/tables/flags.csv");
    remove_file_if_exists("data/tables/metrics.csv");
    remove_file_if_exists("data/tables/notes.csv");
    remove_file_if_exists("data/tables/users.csv");
    remove_file_if_exists("data/tables/broken.csv.tmp");
    remove_file_if_exists("data/tables/dates.csv.tmp");
    remove_file_if_exists("data/tables/datetimes.csv.tmp");
    remove_file_if_exists("data/tables/flags.csv.tmp");
    remove_file_if_exists("data/tables/metrics.csv.tmp");
    remove_file_if_exists("data/tables/notes.csv.tmp");
    remove_file_if_exists("data/tables/users.csv.tmp");
    remove_file_if_exists("data/schema/broken.schema");
    remove_file_if_exists("data/schema/dates.schema");
    remove_file_if_exists("data/schema/datetimes.schema");
    remove_file_if_exists("data/schema/flags.schema");
    remove_file_if_exists("data/schema/metrics.schema");
    remove_file_if_exists("data/schema/notes.schema");
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

static int build_schema_path(const char *table, char *buffer, size_t size)
{
    int written = snprintf(buffer, size, "data/schema/%s.schema", table);
    return (written < 0 || (size_t)written >= size) ? -1 : 0;
}

static int build_table_path(const char *table, char *buffer, size_t size)
{
    int written = snprintf(buffer, size, "data/tables/%s.csv", table);
    return (written < 0 || (size_t)written >= size) ? -1 : 0;
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

static int prepare_table(const char *table, const char *schema_content, const char *table_content)
{
    char schema_path[PATH_SIZE];
    char table_path[PATH_SIZE];

    if (prepare_dirs() != 0) {
        return -1;
    }

    if (build_schema_path(table, schema_path, sizeof(schema_path)) != 0) {
        return -1;
    }

    if (write_text_file(schema_path, schema_content) != 0) {
        return -1;
    }

    if (table_content != NULL) {
        if (build_table_path(table, table_path, sizeof(table_path)) != 0) {
            return -1;
        }

        if (write_text_file(table_path, table_content) != 0) {
            return -1;
        }
    }

    return 0;
}

static WhereClause make_where(const char *column, const char *op, const char *value)
{
    WhereClause where;

    memset(&where, 0, sizeof(where));
    if (column != NULL) {
        snprintf(where.column, sizeof(where.column), "%s", column);
    }
    if (op != NULL) {
        snprintf(where.op, sizeof(where.op), "%s", op);
    }
    if (value != NULL) {
        snprintf(where.value, sizeof(where.value), "%s", value);
    }

    return where;
}

static int call_storage_delete(const char *table, WhereClause *where, int where_count,
                               char **where_links, const char *where_logic)
{
    ParsedSQL sql;

    memset(&sql, 0, sizeof(sql));
    if (table != NULL) {
        snprintf(sql.table, sizeof(sql.table), "%s", table);
    }
    sql.where = where;
    sql.where_count = where_count;
    sql.where_links = where_links;
    if (where_logic != NULL) {
        snprintf(sql.where_logic, sizeof(sql.where_logic), "%s", where_logic);
    }

    return storage_delete(table, &sql);
}

#define storage_delete(table, where, where_count) \
    call_storage_delete((table), (where), (where_count), NULL, NULL)

static int test_delete_all_rows_success(void)
{
    char buffer[BUFFER_SIZE];
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_table("users",
                              "id,INT\nname,VARCHAR\n",
                              "1,kim\n2,lee\n") == 0);
    ASSERT_TRUE(storage_delete("users", NULL, 0) == 0);
    ASSERT_TRUE(read_text_file("data/tables/users.csv", buffer, sizeof(buffer)) == 0);
    ASSERT_TRUE(strcmp(buffer, "") == 0);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_delete_single_equals_success(void)
{
    WhereClause where = make_where("name", "=", "lee");
    char buffer[BUFFER_SIZE];
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_table("users",
                              "id,INT\nname,VARCHAR\n",
                              "1,kim\n2,lee\n3,park\n") == 0);
    ASSERT_TRUE(storage_delete("users", &where, 1) == 0);
    ASSERT_TRUE(read_text_file("data/tables/users.csv", buffer, sizeof(buffer)) == 0);
    ASSERT_TRUE(strcmp(buffer, "1,kim\n3,park\n") == 0);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_delete_int_comparison_success(void)
{
    WhereClause where = make_where("age", ">=", "25");
    char buffer[BUFFER_SIZE];
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_table("users",
                              "id,INT\nage,INT\n",
                              "1,20\n2,25\n3,30\n") == 0);
    ASSERT_TRUE(storage_delete("users", &where, 1) == 0);
    ASSERT_TRUE(read_text_file("data/tables/users.csv", buffer, sizeof(buffer)) == 0);
    ASSERT_TRUE(strcmp(buffer, "1,20\n") == 0);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_delete_float_comparison_success(void)
{
    WhereClause where = make_where("score", "<", "2.0");
    char buffer[BUFFER_SIZE];
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_table("metrics",
                              "id,INT\nscore,FLOAT\n",
                              "1,1.50\n2,2.75\n3,3.10\n") == 0);
    ASSERT_TRUE(storage_delete("metrics", &where, 1) == 0);
    ASSERT_TRUE(read_text_file("data/tables/metrics.csv", buffer, sizeof(buffer)) == 0);
    ASSERT_TRUE(strcmp(buffer, "2,2.75\n3,3.10\n") == 0);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_delete_boolean_comparison_success(void)
{
    WhereClause where = make_where("active", "=", "true");
    char buffer[BUFFER_SIZE];
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_table("flags",
                              "id,INT\nactive,BOOLEAN\n",
                              "1,true\n2,false\n3,1\n") == 0);
    ASSERT_TRUE(storage_delete("flags", &where, 1) == 0);
    ASSERT_TRUE(read_text_file("data/tables/flags.csv", buffer, sizeof(buffer)) == 0);
    ASSERT_TRUE(strcmp(buffer, "2,false\n") == 0);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_delete_date_comparison_success(void)
{
    WhereClause where = make_where("joined", ">=", "2024-03-15");
    char buffer[BUFFER_SIZE];
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_table("dates",
                              "id,INT\njoined,DATE\n",
                              "1,2024-01-01\n2,2024-03-15\n3,2024-04-01\n") == 0);
    ASSERT_TRUE(storage_delete("dates", &where, 1) == 0);
    ASSERT_TRUE(read_text_file("data/tables/dates.csv", buffer, sizeof(buffer)) == 0);
    ASSERT_TRUE(strcmp(buffer, "1,2024-01-01\n") == 0);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_delete_like_success(void)
{
    WhereClause where = make_where("name", "LIKE", "a_i%");
    char buffer[BUFFER_SIZE];
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_table("users",
                              "id,INT\nname,VARCHAR\n",
                              "1,alice\n2,amigo\n3,bob\n") == 0);
    ASSERT_TRUE(storage_delete("users", &where, 1) == 0);
    ASSERT_TRUE(read_text_file("data/tables/users.csv", buffer, sizeof(buffer)) == 0);
    ASSERT_TRUE(strcmp(buffer, "3,bob\n") == 0);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_delete_quoted_field_success(void)
{
    WhereClause where = make_where("note", "=", "kim, \"minsu\"");
    char buffer[BUFFER_SIZE];
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_table("notes",
                              "id,INT\nnote,VARCHAR\n",
                              "1,\"kim, \"\"minsu\"\"\"\n2,plain\n") == 0);
    ASSERT_TRUE(storage_delete("notes", &where, 1) == 0);
    ASSERT_TRUE(read_text_file("data/tables/notes.csv", buffer, sizeof(buffer)) == 0);
    ASSERT_TRUE(strcmp(buffer, "2,plain\n") == 0);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_delete_multiline_field_success(void)
{
    WhereClause where = make_where("note", "=", "hello\nworld");
    char buffer[BUFFER_SIZE];
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_table("notes",
                              "id,INT\nnote,VARCHAR\n",
                              "1,\"hello\nworld\"\n2,plain\n") == 0);
    ASSERT_TRUE(storage_delete("notes", &where, 1) == 0);
    ASSERT_TRUE(read_text_file("data/tables/notes.csv", buffer, sizeof(buffer)) == 0);
    ASSERT_TRUE(strcmp(buffer, "2,plain\n") == 0);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_delete_zero_match_success(void)
{
    WhereClause where = make_where("id", "=", "99");
    char buffer[BUFFER_SIZE];
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_table("users",
                              "id,INT\nname,VARCHAR\n",
                              "1,kim\n2,lee\n") == 0);
    ASSERT_TRUE(storage_delete("users", &where, 1) == 0);
    ASSERT_TRUE(read_text_file("data/tables/users.csv", buffer, sizeof(buffer)) == 0);
    ASSERT_TRUE(strcmp(buffer, "1,kim\n2,lee\n") == 0);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_delete_missing_schema_fails(void)
{
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_dirs() == 0);
    ASSERT_TRUE(write_text_file("data/tables/users.csv", "1,kim\n") == 0);
    ASSERT_TRUE(storage_delete("users", NULL, 0) == -1);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_delete_missing_table_fails(void)
{
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_table("users",
                              "id,INT\nname,VARCHAR\n",
                              NULL) == 0);
    ASSERT_TRUE(storage_delete("users", NULL, 0) == -1);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_delete_multiple_where_and_success(void)
{
    WhereClause where[3];
    char *where_links[] = {"AND", "AND"};
    char buffer[BUFFER_SIZE];
    int status = 1;

    where[0] = make_where("id", ">=", "2");
    where[1] = make_where("age", ">=", "30");
    where[2] = make_where("name", "!=", "park");

    reset_test_environment();
    ASSERT_TRUE(prepare_table("users",
                              "id,INT\nname,VARCHAR\nage,INT\n",
                              "1,kim,20\n2,lee,31\n3,park,28\n4,choi,34\n") == 0);
    ASSERT_TRUE(call_storage_delete("users", where, 3, where_links, NULL) == 0);
    ASSERT_TRUE(read_text_file("data/tables/users.csv", buffer, sizeof(buffer)) == 0);
    ASSERT_TRUE(strcmp(buffer, "1,kim,20\n3,park,28\n") == 0);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_delete_multiple_where_or_success(void)
{
    WhereClause where[3];
    char *where_links[] = {"OR", "OR"};
    char buffer[BUFFER_SIZE];
    int status = 1;

    where[0] = make_where("id", "=", "1");
    where[1] = make_where("name", "=", "park");
    where[2] = make_where("age", ">", "40");

    reset_test_environment();
    ASSERT_TRUE(prepare_table("users",
                              "id,INT\nname,VARCHAR\nage,INT\n",
                              "1,kim,20\n2,lee,31\n3,park,28\n4,choi,34\n") == 0);
    ASSERT_TRUE(call_storage_delete("users", where, 3, where_links, NULL) == 0);
    ASSERT_TRUE(read_text_file("data/tables/users.csv", buffer, sizeof(buffer)) == 0);
    ASSERT_TRUE(strcmp(buffer, "2,lee,31\n4,choi,34\n") == 0);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_delete_multiple_where_mixed_success(void)
{
    WhereClause where[3];
    char *where_links[] = {"OR", "AND"};
    char buffer[BUFFER_SIZE];
    int status = 1;

    where[0] = make_where("name", "=", "kim");
    where[1] = make_where("name", "=", "lee");
    where[2] = make_where("age", ">=", "30");

    reset_test_environment();
    ASSERT_TRUE(prepare_table("users",
                              "id,INT\nname,VARCHAR\nage,INT\n",
                              "1,kim,20\n2,lee,31\n3,park,28\n4,choi,34\n") == 0);
    ASSERT_TRUE(call_storage_delete("users", where, 3, where_links, NULL) == 0);
    ASSERT_TRUE(read_text_file("data/tables/users.csv", buffer, sizeof(buffer)) == 0);
    ASSERT_TRUE(strcmp(buffer, "3,park,28\n4,choi,34\n") == 0);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_delete_multiple_where_fallback_success(void)
{
    WhereClause where[2];
    char buffer[BUFFER_SIZE];
    int status = 1;

    where[0] = make_where("age", ">=", "30");
    where[1] = make_where("name", "=", "park");

    reset_test_environment();
    ASSERT_TRUE(prepare_table("users",
                              "id,INT\nname,VARCHAR\nage,INT\n",
                              "1,kim,20\n2,lee,31\n3,park,28\n4,choi,34\n") == 0);
    ASSERT_TRUE(call_storage_delete("users", where, 2, NULL, "OR") == 0);
    ASSERT_TRUE(read_text_file("data/tables/users.csv", buffer, sizeof(buffer)) == 0);
    ASSERT_TRUE(strcmp(buffer, "1,kim,20\n") == 0);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_delete_unknown_column_fails(void)
{
    WhereClause where = make_where("nickname", "=", "kim");
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_table("users",
                              "id,INT\nname,VARCHAR\n",
                              "1,kim\n") == 0);
    ASSERT_TRUE(storage_delete("users", &where, 1) == -1);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_delete_invalid_operator_fails(void)
{
    WhereClause where = make_where("name", "??", "kim");
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_table("users",
                              "id,INT\nname,VARCHAR\n",
                              "1,kim\n") == 0);
    ASSERT_TRUE(storage_delete("users", &where, 1) == -1);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_delete_malformed_csv_fails(void)
{
    WhereClause where = make_where("note", "=", "broken");
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_table("broken",
                              "id,INT\nnote,VARCHAR\n",
                              "1,\"broken\n2,ok\n") == 0);
    ASSERT_TRUE(storage_delete("broken", &where, 1) == -1);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_delete_row_count_mismatch_fails(void)
{
    WhereClause where = make_where("name", "=", "kim");
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_table("users",
                              "id,INT\nname,VARCHAR\n",
                              "1\n") == 0);
    ASSERT_TRUE(storage_delete("users", &where, 1) == -1);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_delete_datetime_non_equality_fails(void)
{
    WhereClause where = make_where("created_at", ">", "2024-04-08 09:00:00");
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_table("datetimes",
                              "id,INT\ncreated_at,DATETIME\n",
                              "1,2024-04-08 09:00:00\n") == 0);
    ASSERT_TRUE(storage_delete("datetimes", &where, 1) == -1);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

/* ─── Week 7: DELETE 후 B+ 트리 인덱스 동기화 테스트 ─────────── */

/* DELETE 된 id 는 트리에서 사라져야 한다 (-1 반환). */
static int test_delete_index_sync_deleted_id_gone(void)
{
    WhereClause where = make_where("id", "=", "2");
    BPTree *tree;
    int status = 1;

    reset_test_environment();
    index_registry_destroy_all();
    ASSERT_TRUE(prepare_table("users",
                              "id,INT\nname,VARCHAR\n",
                              "1,alice\n2,bob\n3,carol\n") == 0);

    ASSERT_TRUE(storage_delete("users", &where, 1) == 0);

    tree = index_registry_get("users");
    ASSERT_TRUE(tree != NULL);
    ASSERT_TRUE(bptree_search(tree, 2) == -1);  /* 삭제된 id */
    status = 0;

cleanup:
    index_registry_destroy_all();
    reset_test_environment();
    return status;
}

/* DELETE 후 남은 행의 row_idx 가 올바르게 갱신되어야 한다. */
static int test_delete_index_sync_remaining_row_idx(void)
{
    WhereClause where = make_where("id", "=", "1");
    BPTree *tree;
    int status = 1;

    reset_test_environment();
    index_registry_destroy_all();
    ASSERT_TRUE(prepare_table("users",
                              "id,INT\nname,VARCHAR\n",
                              "1,alice\n2,bob\n3,carol\n") == 0);

    /* id=1 삭제 후 id=2 → row 0, id=3 → row 1 */
    ASSERT_TRUE(storage_delete("users", &where, 1) == 0);

    tree = index_registry_get("users");
    ASSERT_TRUE(tree != NULL);
    ASSERT_TRUE(bptree_search(tree, 1) == -1);  /* 삭제된 id */
    ASSERT_TRUE(bptree_search(tree, 2) == 0);   /* 첫 번째 행으로 이동 */
    ASSERT_TRUE(bptree_search(tree, 3) == 1);   /* 두 번째 행으로 이동 */
    status = 0;

cleanup:
    index_registry_destroy_all();
    reset_test_environment();
    return status;
}
