/* executor.c — 파싱된 쿼리를 storage 로 보내는 우체부 (스텁)
 * ============================================================================
 *
 * ▣ 이 파일이 하는 일
 *   파서가 만든 ParsedSQL 을 받아서 "이건 SELECT 니까 storage_select 에 보내고,
 *   저건 INSERT 니까 storage_insert 에 보낸다" 식으로 적절한 함수를 호출한다.
 *
 * ▣ 왜 따로 두지?
 *   파서는 "글자 → 구조체" 만 책임지고, storage 는 "데이터 저장/읽기" 만
 *   책임진다. 그 사이에서 "구조체 보고 어떤 storage 함수 부를지" 를 결정하는
 *   사람이 따로 필요한데, 그게 executor.
 *
 * ▣ 현재 상태
 *   디스패치 (분기) 만 작성된 스텁. case 본문의 실제 동작은
 *   - SELECT       → 석제
 *   - INSERT/UPDATE/DELETE → 원우 또는 세인 (경쟁)
 *   가 채울 예정.
 *
 *   CREATE 분기는 지용이 1차로 작성한 형태가 들어있다.
 * ============================================================================
 */

#include "types.h"
#include "bptree.h"

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define EXECUTOR_PATH_MAX 512
#define EXECUTOR_LINE_MAX 512

/* storage 쪽이 채워 넣는 전역 인덱스 핸들.
 * Week 7 에서는 executor 가 WHERE id = ? 최적화에서만 읽는다. */
BPTree *g_tree = NULL;

static int executor_try_indexed_select(const char *table, ParsedSQL *sql);
static int executor_should_use_id_index(const ParsedSQL *sql);
static int executor_parse_lookup_id(const WhereClause *where, int *out_id);
static int executor_build_schema_path(const char *table, char *out, size_t size);
static int executor_build_table_path(const char *table, char *out, size_t size);
static int executor_path_exists(const char *path);
static int executor_load_schema(const char *schema_path, ColDef **out_schema, int *out_count);
static int executor_parse_schema_definition(const char *text, char *name_out, size_t name_size,
                                            char *type_out, size_t type_size);
static int executor_parse_column_type(const char *text, ColumnType *out_type);
static int executor_load_row_at_index(const char *table_path, int row_index,
                                      int expected_count, char ***out_row);
static int executor_read_csv_record(FILE *fp, char **out_record);
static int executor_parse_csv_record(const char *record, char ***out_fields, int *out_count);
static int executor_append_char(char **buffer, size_t *len, size_t *cap, char ch);
static int executor_push_field(char ***fields, int *field_count,
                               char **field_buffer, size_t *field_len, size_t *field_cap);
static int executor_build_direct_rowset(const ParsedSQL *sql, const ColDef *schema, int schema_count,
                                        char **row, RowSet **out);
static int executor_build_aggregate_rowset(const ParsedSQL *sql, const ColDef *schema, int schema_count,
                                           char **row, RowSet **out);
static int executor_rowset_alloc(RowSet **out, int row_count, int col_count);
static int executor_resolve_selected_columns(const ParsedSQL *sql,
                                             const ColDef *schema, int schema_count,
                                             int **indices_out, int *count_out);
static int executor_is_select_all(const ParsedSQL *sql);
static int executor_find_schema_index(const ColDef *schema, int schema_count, const char *column);
static int executor_equals_ignore_case(const char *left, const char *right);
static char *executor_dup_string(const char *src);
static void executor_free_string_array(char **arr, int count);
static void executor_strip_optional_quotes(const char *input, char *output, size_t output_size);
static char *executor_trim_whitespace(char *text);
static int executor_parse_aggregate_call(const char *col_name, char *fn_out, size_t fn_size,
                                         char *arg_out, size_t arg_size);
static int executor_evaluate_aggregate_for_row(const char *fn, const char *value,
                                               ColumnType type, int has_row,
                                               char *out, size_t out_size);
static int executor_parse_long_value(const char *text, long *out_value);
static int executor_parse_double_value(const char *text, double *out_value);

/* execute: ParsedSQL 을 받아서 종류에 맞는 storage_* 함수를 호출. */
void execute(ParsedSQL *sql) {
    if (!sql) return;

    switch (sql->type) {
        case QUERY_CREATE:
            /* 테이블 만들기: 컬럼 정의들을 storage 로 넘긴다. */
            storage_create(sql->table, sql->col_defs, sql->col_def_count);
            break;

        case QUERY_INSERT:
            /* 새 행 추가: 컬럼명 배열과 값 배열을 storage 로. */
            storage_insert(sql->table, sql->columns, sql->values, sql->val_count);
            break;

        case QUERY_SELECT:
            /* WHERE id = ? 한정으로 B+트리 row index 조회를 먼저 시도한다.
             * 그 외 모든 SELECT 는 기존 storage_select 선형 경로를 유지한다. */
            if (executor_try_indexed_select(sql->table, sql) != 0) {
                storage_select(sql->table, sql);
            }
            break;

        case QUERY_DELETE:
            /* 삭제: WHERE 조건만 storage 에. */
            storage_delete(sql->table, sql);
            break;

        case QUERY_UPDATE:
            /* 수정: SET 와 WHERE 둘 다 필요. */
            storage_update(sql->table, sql);
            break;

        default:
            fprintf(stderr, "[executor] unknown query type\n");
            break;
    }
}

static int executor_try_indexed_select(const char *table, ParsedSQL *sql)
{
    char schema_path[EXECUTOR_PATH_MAX];
    char table_path[EXECUTOR_PATH_MAX];
    ColDef *schema = NULL;
    int schema_count = 0;
    char **row = NULL;
    RowSet *rs = NULL;
    int lookup_id;
    int row_index;
    int handled = -1;

    if (!executor_should_use_id_index(sql) || g_tree == NULL) {
        return -1;
    }

    if (executor_parse_lookup_id(&sql->where[0], &lookup_id) != 0) {
        return -1;
    }

    if (executor_build_schema_path(table, schema_path, sizeof(schema_path)) != 0 ||
        executor_build_table_path(table, table_path, sizeof(table_path)) != 0) {
        return -1;
    }

    if (executor_load_schema(schema_path, &schema, &schema_count) != 0) {
        goto cleanup;
    }

    if (sql->order_by != NULL &&
        executor_find_schema_index(schema, schema_count, sql->order_by->column) < 0) {
        goto cleanup;
    }

    row_index = bptree_search(g_tree, lookup_id);
    if (row_index >= 0 &&
        executor_load_row_at_index(table_path, row_index, schema_count, &row) != 0) {
        goto cleanup;
    }

    if (executor_build_direct_rowset(sql, schema, schema_count, row, &rs) != 0) {
        goto cleanup;
    }

    print_rowset(stdout, rs);
    handled = 0;

cleanup:
    rowset_free(rs);
    executor_free_string_array(row, schema_count);
    free(schema);
    return handled;
}

static int executor_should_use_id_index(const ParsedSQL *sql)
{
    const WhereClause *where;

    if (sql == NULL || sql->where_count != 1 || sql->where == NULL) {
        return 0;
    }

    where = &sql->where[0];
    return executor_equals_ignore_case(where->column, "id") &&
           strcmp(where->op, "=") == 0;
}

static int executor_parse_lookup_id(const WhereClause *where, int *out_id)
{
    char literal[sizeof(where->value)];
    char *trimmed;
    char *end = NULL;
    long parsed;

    if (where == NULL || out_id == NULL) {
        return -1;
    }

    executor_strip_optional_quotes(where->value, literal, sizeof(literal));
    trimmed = executor_trim_whitespace(literal);
    if (trimmed[0] == '\0') {
        return -1;
    }

    errno = 0;
    parsed = strtol(trimmed, &end, 10);
    if (errno != 0 || end == trimmed) {
        return -1;
    }

    while (*end != '\0' && isspace((unsigned char)*end)) {
        end++;
    }

    if (*end != '\0' || parsed < INT_MIN || parsed > INT_MAX) {
        return -1;
    }

    *out_id = (int)parsed;
    return 0;
}

static int executor_build_schema_path(const char *table, char *out, size_t size)
{
    int written;
    char legacy_path[EXECUTOR_PATH_MAX];

    written = snprintf(out, size, "data/schema/%s.schema", table);
    if (written < 0 || (size_t)written >= size) {
        return -1;
    }

    written = snprintf(legacy_path, sizeof(legacy_path), "data/%s.schema", table);
    if (written < 0 || (size_t)written >= sizeof(legacy_path)) {
        return -1;
    }

    if (!executor_path_exists(out) && executor_path_exists(legacy_path)) {
        written = snprintf(out, size, "%s", legacy_path);
        if (written < 0 || (size_t)written >= size) {
            return -1;
        }
    }

    return 0;
}

static int executor_build_table_path(const char *table, char *out, size_t size)
{
    int written;
    char legacy_path[EXECUTOR_PATH_MAX];
    char nested_schema_path[EXECUTOR_PATH_MAX];

    written = snprintf(out, size, "data/tables/%s.csv", table);
    if (written < 0 || (size_t)written >= size) {
        return -1;
    }

    written = snprintf(legacy_path, sizeof(legacy_path), "data/%s.csv", table);
    if (written < 0 || (size_t)written >= sizeof(legacy_path)) {
        return -1;
    }

    written = snprintf(nested_schema_path, sizeof(nested_schema_path),
                       "data/schema/%s.schema", table);
    if (written < 0 || (size_t)written >= sizeof(nested_schema_path)) {
        return -1;
    }

    if (!executor_path_exists(out) &&
        !executor_path_exists(nested_schema_path) &&
        executor_path_exists(legacy_path)) {
        written = snprintf(out, size, "%s", legacy_path);
        if (written < 0 || (size_t)written >= size) {
            return -1;
        }
    }

    return 0;
}

static int executor_path_exists(const char *path)
{
    FILE *fp;

    if (path == NULL || path[0] == '\0') {
        return 0;
    }

    fp = fopen(path, "r");
    if (fp == NULL) {
        return 0;
    }

    fclose(fp);
    return 1;
}

static int executor_load_schema(const char *schema_path, ColDef **out_schema, int *out_count)
{
    FILE *fp;
    ColDef *schema = NULL;
    int schema_count = 0;
    char line[EXECUTOR_LINE_MAX];

    if (schema_path == NULL || out_schema == NULL || out_count == NULL) {
        return -1;
    }

    fp = fopen(schema_path, "r");
    if (fp == NULL) {
        return -1;
    }

    while (fgets(line, sizeof(line), fp) != NULL) {
        char column_name[sizeof(((ColDef *)0)->name)];
        char type_text[64];
        ColumnType type;
        ColDef *grown_schema;

        if (executor_parse_schema_definition(line, column_name, sizeof(column_name),
                                             type_text, sizeof(type_text)) != 0) {
            free(schema);
            fclose(fp);
            return -1;
        }

        if (column_name[0] == '\0') {
            continue;
        }

        if (executor_parse_column_type(type_text, &type) != 0) {
            free(schema);
            fclose(fp);
            return -1;
        }

        grown_schema = realloc(schema, sizeof(*schema) * (size_t)(schema_count + 1));
        if (grown_schema == NULL) {
            free(schema);
            fclose(fp);
            return -1;
        }

        schema = grown_schema;
        memset(&schema[schema_count], 0, sizeof(schema[schema_count]));
        memcpy(schema[schema_count].name, column_name, strlen(column_name) + 1U);
        schema[schema_count].type = type;
        schema_count++;
    }

    fclose(fp);

    if (schema_count == 0) {
        free(schema);
        return -1;
    }

    *out_schema = schema;
    *out_count = schema_count;
    return 0;
}

static int executor_parse_schema_definition(const char *text, char *name_out, size_t name_size,
                                            char *type_out, size_t type_size)
{
    char buffer[EXECUTOR_LINE_MAX];
    char *trimmed;
    char *separator;
    size_t name_length;
    size_t type_length;
    char *type_text;

    if (text == NULL || name_out == NULL || type_out == NULL ||
        name_size == 0U || type_size == 0U) {
        return -1;
    }

    strncpy(buffer, text, sizeof(buffer) - 1U);
    buffer[sizeof(buffer) - 1U] = '\0';

    trimmed = executor_trim_whitespace(buffer);
    if (trimmed[0] == '\0' || trimmed[0] == '#') {
        name_out[0] = '\0';
        type_out[0] = '\0';
        return 0;
    }

    separator = strchr(trimmed, ',');
    if (separator != NULL) {
        *separator = '\0';
        type_text = executor_trim_whitespace(separator + 1);
    } else {
        size_t offset = strcspn(trimmed, " \t");
        if (trimmed[offset] == '\0') {
            return -1;
        }
        separator = trimmed + offset;
        *separator = '\0';
        type_text = executor_trim_whitespace(separator + 1);
    }

    trimmed = executor_trim_whitespace(trimmed);
    if (trimmed[0] == '\0' || type_text[0] == '\0') {
        return -1;
    }

    name_length = strlen(trimmed);
    type_length = strlen(type_text);
    if (name_length == 0U || name_length + 1U > name_size ||
        type_length + 1U > type_size) {
        return -1;
    }

    memcpy(name_out, trimmed, name_length + 1U);
    memcpy(type_out, type_text, type_length + 1U);
    return 0;
}

static int executor_parse_column_type(const char *text, ColumnType *out_type)
{
    if (text == NULL || out_type == NULL) {
        return -1;
    }

    if (executor_equals_ignore_case(text, "INT")) {
        *out_type = TYPE_INT;
    } else if (executor_equals_ignore_case(text, "VARCHAR")) {
        *out_type = TYPE_VARCHAR;
    } else if (executor_equals_ignore_case(text, "FLOAT")) {
        *out_type = TYPE_FLOAT;
    } else if (executor_equals_ignore_case(text, "BOOLEAN")) {
        *out_type = TYPE_BOOLEAN;
    } else if (executor_equals_ignore_case(text, "DATE")) {
        *out_type = TYPE_DATE;
    } else if (executor_equals_ignore_case(text, "DATETIME")) {
        *out_type = TYPE_DATETIME;
    } else {
        return -1;
    }

    return 0;
}

static int executor_load_row_at_index(const char *table_path, int row_index,
                                      int expected_count, char ***out_row)
{
    FILE *fp;
    int current_index = 0;

    if (table_path == NULL || row_index < 0 || expected_count <= 0 || out_row == NULL) {
        return -1;
    }

    fp = fopen(table_path, "r");
    if (fp == NULL) {
        return -1;
    }

    for (;;) {
        char *record = NULL;
        int read_status;

        read_status = executor_read_csv_record(fp, &record);
        if (read_status == 0) {
            break;
        }
        if (read_status < 0) {
            fclose(fp);
            return -1;
        }

        if (current_index == row_index) {
            int row_count = 0;

            if (executor_parse_csv_record(record, out_row, &row_count) != 0) {
                free(record);
                fclose(fp);
                return -1;
            }

            free(record);
            fclose(fp);
            if (row_count != expected_count) {
                executor_free_string_array(*out_row, row_count);
                *out_row = NULL;
                return -1;
            }
            return 0;
        }

        free(record);
        current_index++;
    }

    fclose(fp);
    return -1;
}

static int executor_read_csv_record(FILE *fp, char **out_record)
{
    char *buffer = NULL;
    size_t len = 0;
    size_t cap = 0;
    int saw_any = 0;
    int in_quotes = 0;

    if (fp == NULL || out_record == NULL) {
        return -1;
    }

    for (;;) {
        int ch = fgetc(fp);

        if (ch == EOF) {
            break;
        }

        saw_any = 1;

        if (!in_quotes && (ch == '\n' || ch == '\r')) {
            if (ch == '\r') {
                int next = fgetc(fp);
                if (next != '\n' && next != EOF) {
                    ungetc(next, fp);
                }
            }
            break;
        }

        if (executor_append_char(&buffer, &len, &cap, (char)ch) != 0) {
            free(buffer);
            return -1;
        }

        if (ch == '"') {
            if (in_quotes) {
                int next = fgetc(fp);
                if (next == '"') {
                    saw_any = 1;
                    if (executor_append_char(&buffer, &len, &cap, (char)next) != 0) {
                        free(buffer);
                        return -1;
                    }
                } else {
                    in_quotes = 0;
                    if (next != EOF) {
                        ungetc(next, fp);
                    }
                }
            } else {
                in_quotes = 1;
            }
        }
    }

    if (!saw_any) {
        free(buffer);
        return 0;
    }

    if (in_quotes || executor_append_char(&buffer, &len, &cap, '\0') != 0) {
        free(buffer);
        return -1;
    }

    *out_record = buffer;
    return 1;
}

static int executor_parse_csv_record(const char *record, char ***out_fields, int *out_count)
{
    char **fields = NULL;
    int field_count = 0;
    char *field_buffer = NULL;
    size_t field_len = 0;
    size_t field_cap = 0;
    int in_quotes = 0;
    int just_closed_quote = 0;
    size_t index;

    if (record == NULL || out_fields == NULL || out_count == NULL) {
        return -1;
    }

    for (index = 0;; ++index) {
        char ch = record[index];

        if (in_quotes) {
            if (ch == '\0') {
                free(field_buffer);
                executor_free_string_array(fields, field_count);
                return -1;
            }

            if (ch == '"') {
                if (record[index + 1] == '"') {
                    if (executor_append_char(&field_buffer, &field_len, &field_cap, '"') != 0) {
                        free(field_buffer);
                        executor_free_string_array(fields, field_count);
                        return -1;
                    }
                    index++;
                } else {
                    in_quotes = 0;
                    just_closed_quote = 1;
                }
            } else if (executor_append_char(&field_buffer, &field_len, &field_cap, ch) != 0) {
                free(field_buffer);
                executor_free_string_array(fields, field_count);
                return -1;
            }
            continue;
        }

        if (just_closed_quote) {
            if (ch == ',' || ch == '\0') {
                if (executor_push_field(&fields, &field_count,
                                        &field_buffer, &field_len, &field_cap) != 0) {
                    executor_free_string_array(fields, field_count);
                    return -1;
                }
                just_closed_quote = 0;
                if (ch == '\0') {
                    break;
                }
                continue;
            }

            free(field_buffer);
            executor_free_string_array(fields, field_count);
            return -1;
        }

        if (ch == '"') {
            if (field_len != 0U) {
                free(field_buffer);
                executor_free_string_array(fields, field_count);
                return -1;
            }
            in_quotes = 1;
            continue;
        }

        if (ch == ',' || ch == '\0') {
            if (executor_push_field(&fields, &field_count,
                                    &field_buffer, &field_len, &field_cap) != 0) {
                executor_free_string_array(fields, field_count);
                return -1;
            }
            if (ch == '\0') {
                break;
            }
            continue;
        }

        if (executor_append_char(&field_buffer, &field_len, &field_cap, ch) != 0) {
            free(field_buffer);
            executor_free_string_array(fields, field_count);
            return -1;
        }
    }

    *out_fields = fields;
    *out_count = field_count;
    return 0;
}

static int executor_append_char(char **buffer, size_t *len, size_t *cap, char ch)
{
    char *grown_buffer;
    size_t new_cap;

    if (buffer == NULL || len == NULL || cap == NULL) {
        return -1;
    }

    if (*len + 1U >= *cap) {
        new_cap = (*cap == 0U) ? 64U : (*cap * 2U);
        grown_buffer = realloc(*buffer, new_cap);
        if (grown_buffer == NULL) {
            return -1;
        }
        *buffer = grown_buffer;
        *cap = new_cap;
    }

    (*buffer)[*len] = ch;
    (*len)++;
    return 0;
}

static int executor_push_field(char ***fields, int *field_count,
                               char **field_buffer, size_t *field_len, size_t *field_cap)
{
    char *field_text;
    char **grown_fields;

    if (fields == NULL || field_count == NULL || field_buffer == NULL ||
        field_len == NULL || field_cap == NULL) {
        return -1;
    }

    if (*field_buffer == NULL) {
        field_text = executor_dup_string("");
        if (field_text == NULL) {
            return -1;
        }
    } else {
        if (executor_append_char(field_buffer, field_len, field_cap, '\0') != 0) {
            return -1;
        }
        field_text = *field_buffer;
        *field_buffer = NULL;
        *field_len = 0U;
        *field_cap = 0U;
    }

    grown_fields = realloc(*fields, sizeof(**fields) * (size_t)(*field_count + 1));
    if (grown_fields == NULL) {
        free(field_text);
        return -1;
    }

    *fields = grown_fields;
    (*fields)[*field_count] = field_text;
    (*field_count)++;
    return 0;
}

static int executor_build_direct_rowset(const ParsedSQL *sql, const ColDef *schema, int schema_count,
                                        char **row, RowSet **out)
{
    RowSet *rs = NULL;
    int *selected_indices = NULL;
    int selected_count = 0;
    int row_count = 0;
    int index;

    if (sql == NULL || schema == NULL || out == NULL) {
        return -1;
    }

    if (sql->col_count == 1 && sql->columns != NULL) {
        char fn[16];
        char arg[64];

        if (executor_parse_aggregate_call(sql->columns[0], fn, sizeof(fn), arg, sizeof(arg)) == 0) {
            return executor_build_aggregate_rowset(sql, schema, schema_count, row, out);
        }
    }

    if (executor_resolve_selected_columns(sql, schema, schema_count,
                                          &selected_indices, &selected_count) != 0) {
        return -1;
    }

    if (row != NULL && !(sql->limit == 0)) {
        row_count = 1;
    }

    if (executor_rowset_alloc(&rs, row_count, selected_count) != 0) {
        free(selected_indices);
        return -1;
    }

    for (index = 0; index < selected_count; ++index) {
        rs->col_names[index] = executor_dup_string(schema[selected_indices[index]].name);
        if (rs->col_names[index] == NULL) {
            goto fail;
        }
    }

    if (row_count == 1) {
        rs->rows[0] = calloc((size_t)selected_count, sizeof(*rs->rows[0]));
        if (rs->rows[0] == NULL) {
            goto fail;
        }

        for (index = 0; index < selected_count; ++index) {
            rs->rows[0][index] = executor_dup_string(row[selected_indices[index]]);
            if (rs->rows[0][index] == NULL) {
                goto fail;
            }
        }
    }

    free(selected_indices);
    *out = rs;
    return 0;

fail:
    free(selected_indices);
    rowset_free(rs);
    return -1;
}

static int executor_build_aggregate_rowset(const ParsedSQL *sql, const ColDef *schema, int schema_count,
                                           char **row, RowSet **out)
{
    char fn[16];
    char arg[64];
    int col_index = -1;
    ColumnType col_type = TYPE_VARCHAR;
    char value[256];
    RowSet *rs = NULL;

    if (sql == NULL || schema == NULL || out == NULL) {
        return -1;
    }

    if (executor_parse_aggregate_call(sql->columns[0], fn, sizeof(fn), arg, sizeof(arg)) != 0) {
        return -1;
    }

    if (strcmp(arg, "*") != 0) {
        col_index = executor_find_schema_index(schema, schema_count, arg);
        if (col_index < 0) {
            return -1;
        }
        col_type = schema[col_index].type;
    }

    if (executor_evaluate_aggregate_for_row(fn,
                                            col_index >= 0 && row != NULL ? row[col_index] : NULL,
                                            col_type,
                                            row != NULL,
                                            value, sizeof(value)) != 0) {
        return -1;
    }

    if (executor_rowset_alloc(&rs, 1, 1) != 0) {
        return -1;
    }

    rs->col_names[0] = executor_dup_string(sql->columns[0]);
    if (rs->col_names[0] == NULL) {
        rowset_free(rs);
        return -1;
    }

    rs->rows[0] = calloc(1U, sizeof(*rs->rows[0]));
    if (rs->rows[0] == NULL) {
        rowset_free(rs);
        return -1;
    }

    rs->rows[0][0] = executor_dup_string(value);
    if (rs->rows[0][0] == NULL) {
        rowset_free(rs);
        return -1;
    }

    *out = rs;
    return 0;
}

static int executor_rowset_alloc(RowSet **out, int row_count, int col_count)
{
    RowSet *rs;

    if (out == NULL) {
        return -1;
    }

    *out = NULL;
    rs = calloc(1U, sizeof(*rs));
    if (rs == NULL) {
        return -1;
    }

    rs->row_count = row_count;
    rs->col_count = col_count;

    if (col_count > 0) {
        rs->col_names = calloc((size_t)col_count, sizeof(*rs->col_names));
        if (rs->col_names == NULL) {
            free(rs);
            return -1;
        }
    }

    if (row_count > 0) {
        rs->rows = calloc((size_t)row_count, sizeof(*rs->rows));
        if (rs->rows == NULL) {
            free(rs->col_names);
            free(rs);
            return -1;
        }
    }

    *out = rs;
    return 0;
}

static int executor_resolve_selected_columns(const ParsedSQL *sql,
                                             const ColDef *schema, int schema_count,
                                             int **indices_out, int *count_out)
{
    int *indices;
    int index;

    if (sql == NULL || schema == NULL || indices_out == NULL || count_out == NULL) {
        return -1;
    }

    if (executor_is_select_all(sql)) {
        indices = malloc((size_t)schema_count * sizeof(*indices));
        if (indices == NULL) {
            return -1;
        }

        for (index = 0; index < schema_count; ++index) {
            indices[index] = index;
        }

        *indices_out = indices;
        *count_out = schema_count;
        return 0;
    }

    indices = malloc((size_t)sql->col_count * sizeof(*indices));
    if (indices == NULL) {
        return -1;
    }

    for (index = 0; index < sql->col_count; ++index) {
        indices[index] = executor_find_schema_index(schema, schema_count, sql->columns[index]);
        if (indices[index] < 0) {
            free(indices);
            return -1;
        }
    }

    *indices_out = indices;
    *count_out = sql->col_count;
    return 0;
}

static int executor_is_select_all(const ParsedSQL *sql)
{
    return sql != NULL &&
           (sql->col_count <= 0 ||
            (sql->col_count == 1 && sql->columns != NULL &&
             strcmp(sql->columns[0], "*") == 0));
}

static int executor_find_schema_index(const ColDef *schema, int schema_count, const char *column)
{
    int index;

    if (schema == NULL || column == NULL) {
        return -1;
    }

    for (index = 0; index < schema_count; ++index) {
        if (executor_equals_ignore_case(schema[index].name, column)) {
            return index;
        }
    }

    return -1;
}

static int executor_equals_ignore_case(const char *left, const char *right)
{
    if (left == NULL || right == NULL) {
        return 0;
    }

    while (*left != '\0' && *right != '\0') {
        if (tolower((unsigned char)*left) != tolower((unsigned char)*right)) {
            return 0;
        }
        left++;
        right++;
    }

    return *left == '\0' && *right == '\0';
}

static char *executor_dup_string(const char *src)
{
    const char *text = src == NULL ? "" : src;
    size_t length = strlen(text);
    char *copy = malloc(length + 1U);

    if (copy == NULL) {
        return NULL;
    }

    memcpy(copy, text, length + 1U);
    return copy;
}

static void executor_free_string_array(char **arr, int count)
{
    int index;

    if (arr == NULL) {
        return;
    }

    for (index = 0; index < count; ++index) {
        free(arr[index]);
    }

    free(arr);
}

static void executor_strip_optional_quotes(const char *input, char *output, size_t output_size)
{
    size_t length;
    size_t copy_length;

    if (output == NULL || output_size == 0U) {
        return;
    }

    if (input == NULL) {
        output[0] = '\0';
        return;
    }

    length = strlen(input);
    if (length >= 2U &&
        ((input[0] == '\'' && input[length - 1U] == '\'') ||
         (input[0] == '"' && input[length - 1U] == '"'))) {
        input++;
        length -= 2U;
    }

    copy_length = (length < output_size - 1U) ? length : (output_size - 1U);
    memcpy(output, input, copy_length);
    output[copy_length] = '\0';
}

static char *executor_trim_whitespace(char *text)
{
    char *end;

    while (*text != '\0' && isspace((unsigned char)*text)) {
        text++;
    }

    end = text + strlen(text);
    while (end > text && isspace((unsigned char)end[-1])) {
        end--;
    }

    *end = '\0';
    return text;
}

static int executor_parse_aggregate_call(const char *col_name, char *fn_out, size_t fn_size,
                                         char *arg_out, size_t arg_size)
{
    const char *cursor;
    const char *open_paren;
    const char *close_paren;
    size_t fn_len = 0;
    size_t arg_len = 0;

    if (col_name == NULL || fn_out == NULL || arg_out == NULL) {
        return -1;
    }

    open_paren = strchr(col_name, '(');
    close_paren = strrchr(col_name, ')');
    if (open_paren == NULL || close_paren == NULL || close_paren < open_paren) {
        return -1;
    }

    for (cursor = col_name; cursor < open_paren && fn_len + 1U < fn_size; ++cursor) {
        if (!isspace((unsigned char)*cursor)) {
            fn_out[fn_len++] = (char)toupper((unsigned char)*cursor);
        }
    }
    fn_out[fn_len] = '\0';
    if (fn_len == 0U) {
        return -1;
    }

    if (strcmp(fn_out, "COUNT") != 0 && strcmp(fn_out, "SUM") != 0 &&
        strcmp(fn_out, "AVG") != 0 && strcmp(fn_out, "MIN") != 0 &&
        strcmp(fn_out, "MAX") != 0) {
        return -1;
    }

    for (cursor = open_paren + 1; cursor < close_paren && arg_len + 1U < arg_size; ++cursor) {
        if (!isspace((unsigned char)*cursor)) {
            arg_out[arg_len++] = *cursor;
        }
    }
    arg_out[arg_len] = '\0';
    return (arg_len == 0U) ? -1 : 0;
}

static int executor_evaluate_aggregate_for_row(const char *fn, const char *value,
                                               ColumnType type, int has_row,
                                               char *out, size_t out_size)
{
    long int_value;
    double float_value;

    if (fn == NULL || out == NULL || out_size == 0U) {
        return -1;
    }

    if (strcmp(fn, "COUNT") == 0) {
        snprintf(out, out_size, "%d", has_row ? 1 : 0);
        return 0;
    }

    if (!has_row) {
        if (strcmp(fn, "SUM") == 0 || strcmp(fn, "AVG") == 0) {
            snprintf(out, out_size, "0");
        } else {
            out[0] = '\0';
        }
        return 0;
    }

    if (strcmp(fn, "MIN") == 0 || strcmp(fn, "MAX") == 0) {
        snprintf(out, out_size, "%s", value == NULL ? "" : value);
        return 0;
    }

    if (type == TYPE_INT) {
        if (executor_parse_long_value(value, &int_value) != 0) {
            return -1;
        }
        if (strcmp(fn, "SUM") == 0) {
            snprintf(out, out_size, "%ld", int_value);
        } else {
            snprintf(out, out_size, "%.2f", (double)int_value);
        }
        return 0;
    }

    if (type == TYPE_FLOAT) {
        if (executor_parse_double_value(value, &float_value) != 0) {
            return -1;
        }
        snprintf(out, out_size, "%.2f", float_value);
        return 0;
    }

    return -1;
}

static int executor_parse_long_value(const char *text, long *out_value)
{
    char *end = NULL;
    long value;

    if (text == NULL || out_value == NULL) {
        return -1;
    }

    errno = 0;
    value = strtol(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0') {
        return -1;
    }

    *out_value = value;
    return 0;
}

static int executor_parse_double_value(const char *text, double *out_value)
{
    char *end = NULL;
    double value;

    if (text == NULL || out_value == NULL) {
        return -1;
    }

    errno = 0;
    value = strtod(text, &end);
    if (errno != 0 || end == text || *end != '\0') {
        return -1;
    }

    *out_value = value;
    return 0;
}
