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
static SetClause make_set(const char *column, const char *value);
static int call_storage_update(const char *table, SetClause *set, int set_count,
                               WhereClause *where, int where_count,
                               char **where_links, const char *where_logic);

static int test_update_all_rows_success(void);
static int test_update_single_where_success(void);
static int test_update_multiple_columns_success(void);
static int test_update_float_success(void);
static int test_update_boolean_success(void);
static int test_update_date_success(void);
static int test_update_varchar_quoted_success(void);
static int test_update_datetime_string_success(void);
static int test_update_zero_match_success(void);
static int test_update_missing_schema_fails(void);
static int test_update_missing_table_fails(void);
static int test_update_multiple_where_and_success(void);
static int test_update_multiple_where_or_success(void);
static int test_update_multiple_where_mixed_success(void);
static int test_update_multiple_where_fallback_success(void);
static int test_update_unknown_column_fails(void);
static int test_update_duplicate_set_column_fails(void);
static int test_update_int_type_mismatch_fails(void);
static int test_update_float_type_mismatch_fails(void);
static int test_update_boolean_type_mismatch_fails(void);
static int test_update_date_format_mismatch_fails(void);
static int test_update_malformed_csv_fails(void);
static int test_update_row_count_mismatch_fails(void);
static int test_update_index_sync_row_idx_correct(void);

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
        {"update all rows success", test_update_all_rows_success},
        {"update single where success", test_update_single_where_success},
        {"update multiple columns success", test_update_multiple_columns_success},
        {"update float success", test_update_float_success},
        {"update boolean success", test_update_boolean_success},
        {"update date success", test_update_date_success},
        {"update varchar quoted success", test_update_varchar_quoted_success},
        {"update datetime string success", test_update_datetime_string_success},
        {"update zero match success", test_update_zero_match_success},
        {"update missing schema fails", test_update_missing_schema_fails},
        {"update missing table fails", test_update_missing_table_fails},
        {"update multiple where AND success", test_update_multiple_where_and_success},
        {"update multiple where OR success", test_update_multiple_where_or_success},
        {"update multiple where mixed success", test_update_multiple_where_mixed_success},
        {"update multiple where fallback success", test_update_multiple_where_fallback_success},
        {"update unknown column fails", test_update_unknown_column_fails},
        {"update duplicate set column fails", test_update_duplicate_set_column_fails},
        {"update int type mismatch fails", test_update_int_type_mismatch_fails},
        {"update float type mismatch fails", test_update_float_type_mismatch_fails},
        {"update boolean type mismatch fails", test_update_boolean_type_mismatch_fails},
        {"update date format mismatch fails", test_update_date_format_mismatch_fails},
        {"update malformed csv fails", test_update_malformed_csv_fails},
        {"update row count mismatch fails", test_update_row_count_mismatch_fails},
        {"update index sync: row_idx correct after update", test_update_index_sync_row_idx_correct},
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

    printf("All UPDATE storage tests passed.\n");
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
    remove_file_if_exists("data/tables/events.csv");
    remove_file_if_exists("data/tables/flags.csv");
    remove_file_if_exists("data/tables/metrics.csv");
    remove_file_if_exists("data/tables/notes.csv");
    remove_file_if_exists("data/tables/users.csv");
    remove_file_if_exists("data/tables/broken.csv.tmp");
    remove_file_if_exists("data/tables/dates.csv.tmp");
    remove_file_if_exists("data/tables/events.csv.tmp");
    remove_file_if_exists("data/tables/flags.csv.tmp");
    remove_file_if_exists("data/tables/metrics.csv.tmp");
    remove_file_if_exists("data/tables/notes.csv.tmp");
    remove_file_if_exists("data/tables/users.csv.tmp");
    remove_file_if_exists("data/schema/broken.schema");
    remove_file_if_exists("data/schema/dates.schema");
    remove_file_if_exists("data/schema/events.schema");
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

static SetClause make_set(const char *column, const char *value)
{
    SetClause set;

    memset(&set, 0, sizeof(set));
    if (column != NULL) {
        snprintf(set.column, sizeof(set.column), "%s", column);
    }
    if (value != NULL) {
        snprintf(set.value, sizeof(set.value), "%s", value);
    }

    return set;
}

static int call_storage_update(const char *table, SetClause *set, int set_count,
                               WhereClause *where, int where_count,
                               char **where_links, const char *where_logic)
{
    ParsedSQL sql;

    memset(&sql, 0, sizeof(sql));
    if (table != NULL) {
        snprintf(sql.table, sizeof(sql.table), "%s", table);
    }
    sql.set = set;
    sql.set_count = set_count;
    sql.where = where;
    sql.where_count = where_count;
    sql.where_links = where_links;
    if (where_logic != NULL) {
        snprintf(sql.where_logic, sizeof(sql.where_logic), "%s", where_logic);
    }

    return storage_update(table, &sql);
}

#define storage_update(table, set, set_count, where, where_count) \
    call_storage_update((table), (set), (set_count), (where), (where_count), NULL, NULL)

static int test_update_all_rows_success(void)
{
    SetClause set[] = {make_set("age", "99")};
    char buffer[BUFFER_SIZE];
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_table("users",
                              "id,INT\nage,INT\n",
                              "1,20\n2,31\n") == 0);
    ASSERT_TRUE(storage_update("users", set, 1, NULL, 0) == 0);
    ASSERT_TRUE(read_text_file("data/tables/users.csv", buffer, sizeof(buffer)) == 0);
    ASSERT_TRUE(strcmp(buffer, "1,99\n2,99\n") == 0);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_update_single_where_success(void)
{
    SetClause set[] = {make_set("age", "40")};
    WhereClause where = make_where("name", "=", "lee");
    char buffer[BUFFER_SIZE];
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_table("users",
                              "id,INT\nname,VARCHAR\nage,INT\n",
                              "1,kim,20\n2,lee,31\n3,park,28\n") == 0);
    ASSERT_TRUE(storage_update("users", set, 1, &where, 1) == 0);
    ASSERT_TRUE(read_text_file("data/tables/users.csv", buffer, sizeof(buffer)) == 0);
    ASSERT_TRUE(strcmp(buffer, "1,kim,20\n2,lee,40\n3,park,28\n") == 0);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_update_multiple_columns_success(void)
{
    SetClause set[] = {make_set("name", "seo"), make_set("age", "33")};
    WhereClause where = make_where("id", "=", "1");
    char buffer[BUFFER_SIZE];
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_table("users",
                              "id,INT\nname,VARCHAR\nage,INT\n",
                              "1,kim,20\n2,lee,31\n") == 0);
    ASSERT_TRUE(storage_update("users", set, 2, &where, 1) == 0);
    ASSERT_TRUE(read_text_file("data/tables/users.csv", buffer, sizeof(buffer)) == 0);
    ASSERT_TRUE(strcmp(buffer, "1,seo,33\n2,lee,31\n") == 0);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_update_float_success(void)
{
    SetClause set[] = {make_set("score", "2.75")};
    WhereClause where = make_where("id", "=", "1");
    char buffer[BUFFER_SIZE];
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_table("metrics",
                              "id,INT\nscore,FLOAT\n",
                              "1,1.50\n2,3.10\n") == 0);
    ASSERT_TRUE(storage_update("metrics", set, 1, &where, 1) == 0);
    ASSERT_TRUE(read_text_file("data/tables/metrics.csv", buffer, sizeof(buffer)) == 0);
    ASSERT_TRUE(strcmp(buffer, "1,2.75\n2,3.10\n") == 0);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_update_boolean_success(void)
{
    SetClause set[] = {make_set("active", "false")};
    WhereClause where = make_where("id", "=", "1");
    char buffer[BUFFER_SIZE];
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_table("flags",
                              "id,INT\nactive,BOOLEAN\n",
                              "1,true\n2,false\n") == 0);
    ASSERT_TRUE(storage_update("flags", set, 1, &where, 1) == 0);
    ASSERT_TRUE(read_text_file("data/tables/flags.csv", buffer, sizeof(buffer)) == 0);
    ASSERT_TRUE(strcmp(buffer, "1,false\n2,false\n") == 0);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_update_date_success(void)
{
    SetClause set[] = {make_set("joined", "2024-12-31")};
    WhereClause where = make_where("id", "=", "2");
    char buffer[BUFFER_SIZE];
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_table("dates",
                              "id,INT\njoined,DATE\n",
                              "1,2024-01-01\n2,2024-02-10\n") == 0);
    ASSERT_TRUE(storage_update("dates", set, 1, &where, 1) == 0);
    ASSERT_TRUE(read_text_file("data/tables/dates.csv", buffer, sizeof(buffer)) == 0);
    ASSERT_TRUE(strcmp(buffer, "1,2024-01-01\n2,2024-12-31\n") == 0);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_update_varchar_quoted_success(void)
{
    SetClause set[] = {make_set("note", "lee, \"minsu\"")};
    WhereClause where = make_where("id", "=", "2");
    char buffer[BUFFER_SIZE];
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_table("notes",
                              "id,INT\nnote,VARCHAR\n",
                              "1,plain\n2,hello\n") == 0);
    ASSERT_TRUE(storage_update("notes", set, 1, &where, 1) == 0);
    ASSERT_TRUE(read_text_file("data/tables/notes.csv", buffer, sizeof(buffer)) == 0);
    ASSERT_TRUE(strcmp(buffer, "1,plain\n2,\"lee, \"\"minsu\"\"\"\n") == 0);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_update_datetime_string_success(void)
{
    SetClause set[] = {make_set("occurred_at", "2024-05-01 10:20:30")};
    WhereClause where = make_where("id", "=", "1");
    char buffer[BUFFER_SIZE];
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_table("events",
                              "id,INT\noccurred_at,DATETIME\n",
                              "1,2024-01-01 00:00:00\n2,2024-02-02 12:00:00\n") == 0);
    ASSERT_TRUE(storage_update("events", set, 1, &where, 1) == 0);
    ASSERT_TRUE(read_text_file("data/tables/events.csv", buffer, sizeof(buffer)) == 0);
    ASSERT_TRUE(strcmp(buffer, "1,2024-05-01 10:20:30\n2,2024-02-02 12:00:00\n") == 0);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_update_zero_match_success(void)
{
    SetClause set[] = {make_set("age", "99")};
    WhereClause where = make_where("id", "=", "99");
    char buffer[BUFFER_SIZE];
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_table("users",
                              "id,INT\nage,INT\n",
                              "1,20\n2,31\n") == 0);
    ASSERT_TRUE(storage_update("users", set, 1, &where, 1) == 0);
    ASSERT_TRUE(read_text_file("data/tables/users.csv", buffer, sizeof(buffer)) == 0);
    ASSERT_TRUE(strcmp(buffer, "1,20\n2,31\n") == 0);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_update_missing_schema_fails(void)
{
    SetClause set[] = {make_set("age", "22")};
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_dirs() == 0);
    ASSERT_TRUE(storage_update("users", set, 1, NULL, 0) == -1);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_update_missing_table_fails(void)
{
    SetClause set[] = {make_set("age", "22")};
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_table("users", "id,INT\nage,INT\n", NULL) == 0);
    ASSERT_TRUE(storage_update("users", set, 1, NULL, 0) == -1);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_update_multiple_where_and_success(void)
{
    SetClause set[] = {make_set("age", "55")};
    WhereClause where[] = {
        make_where("id", ">=", "2"),
        make_where("age", ">=", "30"),
        make_where("name", "!=", "park")
    };
    char *where_links[] = {"AND", "AND"};
    char buffer[BUFFER_SIZE];
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_table("users",
                              "id,INT\nname,VARCHAR\nage,INT\n",
                              "1,kim,20\n2,lee,31\n3,park,28\n4,choi,34\n") == 0);
    ASSERT_TRUE(call_storage_update("users", set, 1, where, 3, where_links, NULL) == 0);
    ASSERT_TRUE(read_text_file("data/tables/users.csv", buffer, sizeof(buffer)) == 0);
    ASSERT_TRUE(strcmp(buffer, "1,kim,20\n2,lee,55\n3,park,28\n4,choi,55\n") == 0);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_update_multiple_where_or_success(void)
{
    SetClause set[] = {make_set("age", "11")};
    WhereClause where[] = {
        make_where("id", "=", "1"),
        make_where("name", "=", "park"),
        make_where("age", ">", "40")
    };
    char *where_links[] = {"OR", "OR"};
    char buffer[BUFFER_SIZE];
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_table("users",
                              "id,INT\nname,VARCHAR\nage,INT\n",
                              "1,kim,20\n2,lee,31\n3,park,28\n4,choi,34\n") == 0);
    ASSERT_TRUE(call_storage_update("users", set, 1, where, 3, where_links, NULL) == 0);
    ASSERT_TRUE(read_text_file("data/tables/users.csv", buffer, sizeof(buffer)) == 0);
    ASSERT_TRUE(strcmp(buffer, "1,kim,11\n2,lee,31\n3,park,11\n4,choi,34\n") == 0);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_update_multiple_where_mixed_success(void)
{
    SetClause set[] = {make_set("age", "77")};
    WhereClause where[] = {
        make_where("name", "=", "kim"),
        make_where("name", "=", "lee"),
        make_where("age", ">=", "30")
    };
    char *where_links[] = {"OR", "AND"};
    char buffer[BUFFER_SIZE];
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_table("users",
                              "id,INT\nname,VARCHAR\nage,INT\n",
                              "1,kim,20\n2,lee,31\n3,park,28\n4,choi,34\n") == 0);
    ASSERT_TRUE(call_storage_update("users", set, 1, where, 3, where_links, NULL) == 0);
    ASSERT_TRUE(read_text_file("data/tables/users.csv", buffer, sizeof(buffer)) == 0);
    ASSERT_TRUE(strcmp(buffer, "1,kim,77\n2,lee,77\n3,park,28\n4,choi,34\n") == 0);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_update_multiple_where_fallback_success(void)
{
    SetClause set[] = {make_set("age", "88")};
    WhereClause where[] = {
        make_where("age", ">=", "30"),
        make_where("name", "=", "park")
    };
    char buffer[BUFFER_SIZE];
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_table("users",
                              "id,INT\nname,VARCHAR\nage,INT\n",
                              "1,kim,20\n2,lee,31\n3,park,28\n4,choi,34\n") == 0);
    ASSERT_TRUE(call_storage_update("users", set, 1, where, 2, NULL, "OR") == 0);
    ASSERT_TRUE(read_text_file("data/tables/users.csv", buffer, sizeof(buffer)) == 0);
    ASSERT_TRUE(strcmp(buffer, "1,kim,20\n2,lee,88\n3,park,88\n4,choi,88\n") == 0);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_update_unknown_column_fails(void)
{
    SetClause set[] = {make_set("nickname", "kim")};
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_table("users", "id,INT\nname,VARCHAR\n", "1,kim\n") == 0);
    ASSERT_TRUE(storage_update("users", set, 1, NULL, 0) == -1);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_update_duplicate_set_column_fails(void)
{
    SetClause set[] = {make_set("age", "22"), make_set("age", "30")};
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_table("users", "id,INT\nage,INT\n", "1,20\n") == 0);
    ASSERT_TRUE(storage_update("users", set, 2, NULL, 0) == -1);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_update_int_type_mismatch_fails(void)
{
    SetClause set[] = {make_set("age", "hello")};
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_table("users", "id,INT\nage,INT\n", "1,20\n") == 0);
    ASSERT_TRUE(storage_update("users", set, 1, NULL, 0) == -1);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_update_float_type_mismatch_fails(void)
{
    SetClause set[] = {make_set("score", "hello")};
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_table("metrics", "id,INT\nscore,FLOAT\n", "1,1.50\n") == 0);
    ASSERT_TRUE(storage_update("metrics", set, 1, NULL, 0) == -1);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_update_boolean_type_mismatch_fails(void)
{
    SetClause set[] = {make_set("active", "maybe")};
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_table("flags", "id,INT\nactive,BOOLEAN\n", "1,true\n") == 0);
    ASSERT_TRUE(storage_update("flags", set, 1, NULL, 0) == -1);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_update_date_format_mismatch_fails(void)
{
    SetClause set[] = {make_set("joined", "2024/12/31")};
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_table("dates", "id,INT\njoined,DATE\n", "1,2024-01-01\n") == 0);
    ASSERT_TRUE(storage_update("dates", set, 1, NULL, 0) == -1);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_update_malformed_csv_fails(void)
{
    SetClause set[] = {make_set("name", "kim")};
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_table("broken",
                              "id,INT\nname,VARCHAR\n",
                              "1,\"unterminated\n") == 0);
    ASSERT_TRUE(storage_update("broken", set, 1, NULL, 0) == -1);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

static int test_update_row_count_mismatch_fails(void)
{
    SetClause set[] = {make_set("age", "22")};
    int status = 1;

    reset_test_environment();
    ASSERT_TRUE(prepare_table("broken",
                              "id,INT\nage,INT\nname,VARCHAR\n",
                              "1,20\n") == 0);
    ASSERT_TRUE(storage_update("broken", set, 1, NULL, 0) == -1);
    status = 0;

cleanup:
    reset_test_environment();
    return status;
}

/* ─── Week 7: UPDATE 후 B+ 트리 인덱스 동기화 테스트 ─────────── */

/* UPDATE 후 트리의 row_idx 가 올바르게 유지되어야 한다. */
static int test_update_index_sync_row_idx_correct(void)
{
    SetClause set = make_set("name", "updated");
    WhereClause where = make_where("id", "=", "2");
    BPTree *tree;
    int status = 1;

    reset_test_environment();
    index_registry_destroy_all();
    ASSERT_TRUE(prepare_table("users",
                              "id,INT\nname,VARCHAR\n",
                              "1,alice\n2,bob\n3,carol\n") == 0);

    ASSERT_TRUE(call_storage_update("users", &set, 1, &where, 1, NULL, NULL) == 0);

    tree = index_registry_get("users");
    ASSERT_TRUE(tree != NULL);
    /* row_idx 는 변경 없이 유지 */
    ASSERT_TRUE(bptree_search(tree, 1) == 0);
    ASSERT_TRUE(bptree_search(tree, 2) == 1);
    ASSERT_TRUE(bptree_search(tree, 3) == 2);
    status = 0;

cleanup:
    index_registry_destroy_all();
    reset_test_environment();
    return status;
}
