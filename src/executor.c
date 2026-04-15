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
#include "index_registry.h"

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int executor_try_indexed_select(const char *table, ParsedSQL *sql);
static int executor_try_range_select(const char *table, ParsedSQL *sql);
static int executor_should_use_id_range(const ParsedSQL *sql);
static int executor_should_use_id_index(const ParsedSQL *sql);
static int executor_parse_lookup_id(const WhereClause *where, int *out_id);
static int executor_parse_between_bounds(const WhereClause *where, int *out_from, int *out_to);
static int executor_parse_int_literal(const char *input, int *out_value);
static void executor_print_rowset_and_free(RowSet *rs);
static void executor_strip_optional_quotes(const char *input, char *output, size_t output_size);
static char *executor_trim_whitespace(char *text);
static int executor_equals_ignore_case(const char *left, const char *right);

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
            /* subprocess 모델 대응: 첫 SELECT 진입 시 CSV → B+ 트리 lazy rebuild.
             * 프로세스당 테이블당 1회만 실제 동작, 이후 no-op. */
            storage_ensure_index(sql->table);
            /* WHERE id BETWEEN ? AND ? 를 가장 먼저 시도하고,
             * 그 다음 WHERE id = ? 단건 인덱스 경로를 탄다. */
            if (executor_try_range_select(sql->table, sql) != 0 &&
                executor_try_indexed_select(sql->table, sql) != 0) {
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

static int executor_try_range_select(const char *table, ParsedSQL *sql)
{
    BPTree *tree;
    RowSet *rs = NULL;
    int from;
    int to;
    int *row_indices = NULL;
    size_t capacity = 16U;
    int count;

    if (!executor_should_use_id_range(sql)) {
        return -1;
    }

    if (executor_parse_between_bounds(&sql->where[0], &from, &to) != 0) {
        return -1;
    }

    tree = index_registry_get(table);
    if (tree == NULL) {
        return -1;
    }

    row_indices = malloc(sizeof(*row_indices) * capacity);
    if (row_indices == NULL) {
        return -1;
    }

    for (;;) {
        size_t new_capacity;
        int *grown;

        count = bptree_range(tree, from, to, row_indices, (int)capacity);
        if ((size_t)count < capacity) {
            break;
        }

        if (capacity > (size_t)INT_MAX / 2U) {
            free(row_indices);
            return -1;
        }

        new_capacity = capacity * 2U;
        grown = realloc(row_indices, sizeof(*row_indices) * new_capacity);
        if (grown == NULL) {
            free(row_indices);
            return -1;
        }

        row_indices = grown;
        capacity = new_capacity;
    }

    if (storage_select_result_by_row_indices(table, sql, row_indices, count, &rs) != 0) {
        free(row_indices);
        rowset_free(rs);
        return -1;
    }

    free(row_indices);
    executor_print_rowset_and_free(rs);
    return 0;
}

static int executor_try_indexed_select(const char *table, ParsedSQL *sql)
{
    BPTree *tree;
    RowSet *rs = NULL;
    int lookup_id;
    int row_index;

    if (!executor_should_use_id_index(sql)) {
        return -1;
    }

    if (executor_parse_lookup_id(&sql->where[0], &lookup_id) != 0) {
        return -1;
    }

    tree = index_registry_get(table);
    if (tree == NULL) {
        return -1;
    }

    row_index = bptree_search(tree, lookup_id);
    if (storage_select_result_by_row_index(table, sql, row_index, &rs) != 0) {
        rowset_free(rs);
        return -1;
    }

    executor_print_rowset_and_free(rs);
    return 0;
}

static int executor_should_use_id_range(const ParsedSQL *sql)
{
    const WhereClause *where;

    if (sql == NULL || sql->where_count != 1 || sql->where == NULL) {
        return 0;
    }

    where = &sql->where[0];
    return executor_equals_ignore_case(where->column, "id") &&
           executor_equals_ignore_case(where->op, "BETWEEN");
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
    if (where == NULL || out_id == NULL) {
        return -1;
    }

    return executor_parse_int_literal(where->value, out_id);
}

static int executor_parse_between_bounds(const WhereClause *where, int *out_from, int *out_to)
{
    int from;
    int to;

    if (where == NULL || out_from == NULL || out_to == NULL) {
        return -1;
    }

    if (executor_parse_int_literal(where->value, &from) != 0 ||
        executor_parse_int_literal(where->value_to, &to) != 0) {
        return -1;
    }

    if (from > to) {
        int temp = from;
        from = to;
        to = temp;
    }

    *out_from = from;
    *out_to = to;
    return 0;
}

static int executor_parse_int_literal(const char *input, int *out_value)
{
    char literal[256];
    char *trimmed;
    char *end = NULL;
    long parsed;

    if (input == NULL || out_value == NULL) {
        return -1;
    }

    executor_strip_optional_quotes(input, literal, sizeof(literal));
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

    *out_value = (int)parsed;
    return 0;
}

static void executor_print_rowset_and_free(RowSet *rs)
{
    if (rs != NULL) {
        print_rowset(stdout, rs);
    }
    rowset_free(rs);
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
        input += 1;
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
