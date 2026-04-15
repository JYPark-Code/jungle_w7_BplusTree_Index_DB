/* test_parser.c — parse_sql 단위 테스트 (지용)
 *
 * 가벼운 자체 assert 기반. 실패하면 비정상 종료.
 */

#define _GNU_SOURCE

#include "types.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int run_executor_tests(void);

static int g_pass = 0;
static int g_fail = 0;

#define CHECK(cond, msg) do {                                            \
    if (cond) { g_pass++; }                                              \
    else      { g_fail++; fprintf(stderr, "  FAIL: %s\n", msg); }        \
} while (0)

#define SECTION(name) fprintf(stderr, "[%s]\n", name)

static void test_create_table(void) {
    SECTION("CREATE TABLE");
    ParsedSQL *s = parse_sql("CREATE TABLE users (id INT, name VARCHAR, joined DATE)");
    CHECK(s != NULL, "parse_sql returned NULL");
    CHECK(s->type == QUERY_CREATE, "type != CREATE");
    CHECK(strcmp(s->table, "users") == 0, "table name");
    CHECK(s->col_def_count == 3, "col_def_count");
    CHECK(strcmp(s->col_defs[0], "id INT") == 0, "col_defs[0]");
    CHECK(strcmp(s->col_defs[1], "name VARCHAR") == 0, "col_defs[1]");
    CHECK(strcmp(s->col_defs[2], "joined DATE") == 0, "col_defs[2]");
    free_parsed(s);
}

static void test_create_all_types(void) {
    SECTION("CREATE TABLE — 6 ColumnTypes");
    ParsedSQL *s = parse_sql(
        "CREATE TABLE t (a INT, b VARCHAR, c FLOAT, d BOOLEAN, e DATE, f DATETIME)");
    CHECK(s->col_def_count == 6, "6 columns");
    CHECK(strstr(s->col_defs[5], "DATETIME") != NULL, "DATETIME accepted");
    free_parsed(s);
}

static void test_insert(void) {
    SECTION("INSERT");
    ParsedSQL *s = parse_sql("INSERT INTO users (id, name) VALUES (1, 'alice')");
    CHECK(s->type == QUERY_INSERT, "type");
    CHECK(strcmp(s->table, "users") == 0, "table");
    CHECK(s->col_count == 2 && s->val_count == 2, "counts");
    CHECK(strcmp(s->columns[0], "id") == 0, "col[0]");
    CHECK(strcmp(s->values[1], "alice") == 0, "val[1] (quote stripped)");
    free_parsed(s);
}

static void test_select_star(void) {
    SECTION("SELECT *");
    ParsedSQL *s = parse_sql("SELECT * FROM users");
    CHECK(s->type == QUERY_SELECT, "type");
    CHECK(s->col_count == 1 && strcmp(s->columns[0], "*") == 0, "*");
    CHECK(strcmp(s->table, "users") == 0, "table");
    free_parsed(s);
}

static void test_select_where_order_limit(void) {
    SECTION("SELECT col WHERE ... ORDER BY ... LIMIT");
    ParsedSQL *s = parse_sql(
        "SELECT id, name FROM users WHERE age > 20 ORDER BY name DESC LIMIT 10");
    CHECK(s->col_count == 2, "col_count");
    CHECK(s->where_count == 1, "where_count");
    CHECK(strcmp(s->where[0].column, "age") == 0, "where col");
    CHECK(strcmp(s->where[0].op, ">") == 0, "where op");
    CHECK(strcmp(s->where[0].value, "20") == 0, "where val");
    CHECK(s->order_by != NULL && strcmp(s->order_by->column, "name") == 0, "order col");
    CHECK(s->order_by->asc == 0, "DESC");
    CHECK(s->limit == 10, "limit");
    free_parsed(s);
}

static void test_where_and(void) {
    SECTION("WHERE AND");
    ParsedSQL *s = parse_sql("SELECT * FROM users WHERE age > 20 AND name = 'bob'");
    CHECK(s->where_count == 2, "where_count == 2");
    CHECK(strcmp(s->where_logic, "AND") == 0, "logic AND");
    CHECK(s->where_links != NULL, "where_links allocated");
    CHECK(strcmp(s->where_links[0], "AND") == 0, "where_links[0] AND");
    CHECK(strcmp(s->where[1].value, "bob") == 0, "second cond value");
    free_parsed(s);
}

/* AND로만 이어진 3개 조건을 모두 읽고, 호환용 where_logic도 채우는지 본다. */
static void test_where_nary_and(void) {
    SECTION("WHERE AND (3 conditions)");
    ParsedSQL *s = parse_sql(
        "SELECT * FROM users WHERE age > 20 AND score >= 80 AND city = 'Seoul'");
    CHECK(s->where_count == 3, "where_count == 3");
    CHECK(s->where_links != NULL, "where_links allocated");
    CHECK(strcmp(s->where_links[0], "AND") == 0, "link[0] AND");
    CHECK(strcmp(s->where_links[1], "AND") == 0, "link[1] AND");
    CHECK(strcmp(s->where_logic, "AND") == 0, "uniform AND fallback");
    CHECK(strcmp(s->where[2].column, "city") == 0, "third cond column");
    free_parsed(s);
}

/* AND/OR가 섞인 경우에는 링크 배열만 믿고, 옛 where_logic은 비워 두는지 본다. */
static void test_where_nary_mixed_logic(void) {
    SECTION("WHERE mixed logic (3 conditions)");
    ParsedSQL *s = parse_sql(
        "SELECT * FROM users WHERE age > 20 AND name = 'bob' OR city = 'Seoul'");
    CHECK(s->where_count == 3, "where_count == 3");
    CHECK(s->where_links != NULL, "where_links allocated");
    CHECK(strcmp(s->where_links[0], "AND") == 0, "link[0] AND");
    CHECK(strcmp(s->where_links[1], "OR") == 0, "link[1] OR");
    CHECK(s->where_logic[0] == '\0', "mixed logic leaves deprecated field empty");
    CHECK(strcmp(s->where[2].value, "Seoul") == 0, "third cond value");
    free_parsed(s);
}

static void test_delete(void) {
    SECTION("DELETE");
    ParsedSQL *s = parse_sql("DELETE FROM users WHERE id = 5");
    CHECK(s->type == QUERY_DELETE, "type");
    CHECK(strcmp(s->table, "users") == 0, "table");
    CHECK(s->where_count == 1, "where_count");
    CHECK(strcmp(s->where[0].value, "5") == 0, "value");
    free_parsed(s);
}

static void test_update(void) {
    SECTION("UPDATE");
    ParsedSQL *s = parse_sql("UPDATE users SET name = 'carol', age = 30 WHERE id = 1");
    CHECK(s->type == QUERY_UPDATE, "type");
    CHECK(s->set_count == 2, "set_count");
    CHECK(strcmp(s->set[0].column, "name") == 0, "set[0].col");
    CHECK(strcmp(s->set[0].value, "carol") == 0, "set[0].val");
    CHECK(strcmp(s->set[1].column, "age") == 0, "set[1].col");
    CHECK(s->where_count == 1, "where_count");
    free_parsed(s);
}

static void test_delete_mixed_where(void) {
    SECTION("DELETE mixed WHERE");
    ParsedSQL *s = parse_sql("DELETE FROM users WHERE a = 1 AND b = 2 OR c = 3");
    CHECK(s->type == QUERY_DELETE, "type");
    CHECK(s->where_count == 3, "where_count");
    CHECK(s->where_links != NULL, "where_links");
    CHECK(strcmp(s->where_links[0], "AND") == 0, "first link");
    CHECK(strcmp(s->where_links[1], "OR") == 0, "second link");
    free_parsed(s);
}

static void test_update_mixed_where(void) {
    SECTION("UPDATE mixed WHERE");
    ParsedSQL *s = parse_sql("UPDATE users SET age = 30 WHERE a = 1 OR b = 2 AND c = 3");
    CHECK(s->type == QUERY_UPDATE, "type");
    CHECK(s->set_count == 1, "set_count");
    CHECK(s->where_count == 3, "where_count");
    CHECK(s->where_links != NULL, "where_links");
    CHECK(strcmp(s->where_links[0], "OR") == 0, "first link");
    CHECK(strcmp(s->where_links[1], "AND") == 0, "second link");
    free_parsed(s);
}

static void test_free_null(void) {
    SECTION("free_parsed(NULL)");
    free_parsed(NULL);    /* must not crash */
    g_pass++;
}

static void test_select_count_star(void) {
    SECTION("SELECT COUNT(*) FROM table");
    ParsedSQL *s = parse_sql("SELECT COUNT(*) FROM users");
    CHECK(s->type == QUERY_SELECT, "type SELECT");
    CHECK(strcmp(s->table, "users") == 0, "table");
    CHECK(s->col_count == 1, "1 column");
    CHECK(strcmp(s->columns[0], "COUNT(*)") == 0, "COUNT(*) 한 컬럼으로 묶임");
    free_parsed(s);
}

static void test_select_count_star_spaced(void) {
    SECTION("SELECT COUNT ( * ) FROM table — 공백 변형");
    ParsedSQL *s = parse_sql("SELECT COUNT ( * ) FROM users");
    CHECK(s->col_count == 1, "1 column");
    /* 공백 없이 이어붙어야 storage 의 normalized 비교가 잘 작동 */
    CHECK(strcmp(s->columns[0], "COUNT(*)") == 0, "공백 제거되어 COUNT(*)");
    free_parsed(s);
}

static void test_select_count_with_where(void) {
    SECTION("SELECT COUNT(*) FROM t WHERE ...");
    ParsedSQL *s = parse_sql("SELECT COUNT(*) FROM users WHERE age > 20");
    CHECK(s->col_count == 1, "1 column");
    CHECK(strcmp(s->columns[0], "COUNT(*)") == 0, "COUNT(*)");
    CHECK(s->where_count == 1, "WHERE 1개");
    CHECK(strcmp(s->where[0].column, "age") == 0, "WHERE col");
    free_parsed(s);
}

static void test_select_function_with_arg(void) {
    SECTION("SELECT SUM(price) FROM t — 일반 함수 호출형");
    ParsedSQL *s = parse_sql("SELECT SUM(price) FROM orders");
    CHECK(s->col_count == 1, "1 column");
    CHECK(strcmp(s->columns[0], "SUM(price)") == 0, "SUM(price) 한 컬럼");
    free_parsed(s);
}

/* COUNT(*) 에서 쓰던 함수 호출형 컬럼 결합이 다른 집계 함수에도 그대로
 * 적용되는지 확인한다. */
static void test_select_aggregate_variants(void) {
    SECTION("SELECT aggregate variants");
    ParsedSQL *s = parse_sql(
        "SELECT SUM(price), AVG(age), MIN(joined), MAX(score) FROM orders");
    CHECK(s->col_count == 4, "4 aggregate columns");
    CHECK(strcmp(s->columns[0], "SUM(price)") == 0, "SUM(price)");
    CHECK(strcmp(s->columns[1], "AVG(age)") == 0, "AVG(age)");
    CHECK(strcmp(s->columns[2], "MIN(joined)") == 0, "MIN(joined)");
    CHECK(strcmp(s->columns[3], "MAX(score)") == 0, "MAX(score)");
    CHECK(strcmp(s->table, "orders") == 0, "table orders");
    free_parsed(s);
}

/* 세미콜론 같은 stop set 이 컬럼 목록 뒤에 나와도 가짜 컬럼으로
 * 잘못 추가되지 않는지 확인한다. */
static void test_select_stop_set_semicolon(void) {
    SECTION("SELECT stop set blocks semicolon as column");
    ParsedSQL *s = parse_sql("SELECT SUM(price), ;");
    CHECK(s->col_count == 1, "semicolon not parsed as phantom column");
    CHECK(strcmp(s->columns[0], "SUM(price)") == 0, "aggregate column kept");
    free_parsed(s);
}

/* ─── 엣지 케이스 ────────────────────────────────────────── */

static void test_empty_input(void) {
    SECTION("빈 입력");
    CHECK(parse_sql("") == NULL,            "empty string → NULL");
    CHECK(parse_sql(NULL) == NULL,          "NULL input → NULL");
    ParsedSQL *s = parse_sql("   \n\t  ");
    CHECK(s == NULL,                        "whitespace only → NULL");
    free_parsed(s);
}

static void test_unknown_keyword(void) {
    SECTION("알 수 없는 키워드");
    ParsedSQL *s = parse_sql("DROP TABLE users");
    CHECK(s != NULL,                        "non-NULL on unknown");
    CHECK(s->type == QUERY_UNKNOWN,         "type UNKNOWN");
    free_parsed(s);
}

static void test_case_insensitive(void) {
    SECTION("대소문자 무관 키워드");
    ParsedSQL *s1 = parse_sql("select * from users where id = 1");
    CHECK(s1->type == QUERY_SELECT,         "lowercase select");
    CHECK(s1->where_count == 1,             "lowercase where");
    free_parsed(s1);

    ParsedSQL *s2 = parse_sql("Create TaBLe T (id Int)");
    CHECK(s2->type == QUERY_CREATE,         "mixed case Create");
    CHECK(s2->col_def_count == 1,           "mixed case col");
    free_parsed(s2);
}

static void test_default_limit(void) {
    SECTION("LIMIT 없을 때 기본값 -1");
    ParsedSQL *s = parse_sql("SELECT * FROM users");
    CHECK(s->limit == -1,                   "no LIMIT → -1");
    free_parsed(s);
}

static void test_invalid_create_type(void) {
    SECTION("CREATE TABLE 잘못된 타입은 경고만, 파싱 계속");
    /* stderr 에 경고가 찍히지만 구조는 채워짐 */
    ParsedSQL *s = parse_sql("CREATE TABLE t (id BANANA, name VARCHAR)");
    CHECK(s->type == QUERY_CREATE,          "still CREATE");
    CHECK(s->col_def_count == 2,            "both col_defs stored");
    CHECK(strstr(s->col_defs[0], "BANANA") != NULL, "invalid type kept in col_def");
    free_parsed(s);
}

static void test_where_operators(void) {
    SECTION("다양한 WHERE 연산자");
    const char *ops[] = {"=", ">", "<", ">=", "<=", "!="};
    for (int i = 0; i < 6; i++) {
        char buf[128];
        snprintf(buf, sizeof(buf), "SELECT * FROM t WHERE x %s 1", ops[i]);
        ParsedSQL *s = parse_sql(buf);
        CHECK(s->where_count == 1,          "where count");
        CHECK(strcmp(s->where[0].op, ops[i]) == 0, ops[i]);
        free_parsed(s);
    }
}

static void test_trailing_whitespace(void) {
    SECTION("끝 공백/개행");
    ParsedSQL *s = parse_sql("SELECT * FROM users   \n\t  ");
    CHECK(s->type == QUERY_SELECT,          "type ok");
    CHECK(strcmp(s->table, "users") == 0,   "table ok");
    free_parsed(s);
}

static void test_select_or_where(void) {
    SECTION("WHERE OR (2 conditions)");
    ParsedSQL *s = parse_sql("SELECT * FROM users WHERE age < 20 OR age > 60");
    CHECK(s->where_count == 2,              "2 conditions");
    CHECK(strcmp(s->where_logic, "OR") == 0,"OR logic");
    CHECK(s->where_links != NULL,            "where_links allocated");
    CHECK(strcmp(s->where_links[0], "OR") == 0, "where_links[0] OR");
    free_parsed(s);
}

static void test_where_mixed_links(void) {
    SECTION("WHERE mixed AND/OR links");
    ParsedSQL *s = parse_sql("SELECT * FROM users WHERE a = 1 AND b = 2 OR c = 3");
    CHECK(s->where_count == 3, "where_count == 3");
    CHECK(s->where_links != NULL, "where_links allocated");
    CHECK(strcmp(s->where_links[0], "AND") == 0, "first link AND");
    CHECK(strcmp(s->where_links[1], "OR") == 0, "second link OR");
    CHECK(s->where_logic[0] == '\0', "mixed links clear fallback logic");
    free_parsed(s);
}

static void test_order_by_asc_explicit(void) {
    SECTION("ORDER BY ... ASC (명시)");
    ParsedSQL *s = parse_sql("SELECT * FROM users ORDER BY name ASC");
    CHECK(s->order_by != NULL && s->order_by->asc == 1, "ASC");
    free_parsed(s);
}

static void test_sql_line_comment(void) {
    SECTION("SQL 라인 주석 (-- ...)");
    ParsedSQL *s = parse_sql(
        "-- this is a comment\n"
        "CREATE TABLE t (id INT)");
    CHECK(s->type == QUERY_CREATE, "comment skipped, CREATE parsed");
    CHECK(strcmp(s->table, "t") == 0, "table after comment");
    CHECK(s->col_def_count == 1, "col after comment");
    free_parsed(s);

    /* 중간 주석 */
    ParsedSQL *s2 = parse_sql(
        "SELECT id -- pick id\n"
        "FROM users");
    CHECK(s2->type == QUERY_SELECT, "mid-comment SELECT");
    CHECK(strcmp(s2->table, "users") == 0, "table after mid comment");
    free_parsed(s2);
}

/* ─── AST 출력 (print_ast) 테스트 ─────────────────────────── */

/* AST 출력 결과를 메모리 버퍼로 받아서 문자열 단위로 검증한다. */
static char *capture_ast(const char *sql_text) {
    ParsedSQL *sql = parse_sql(sql_text);
    char  *buf = NULL;
    size_t len = 0;
    FILE  *out = open_memstream(&buf, &len);
    print_ast(out, sql);
    fclose(out);
    free_parsed(sql);
    return buf;
}

static void test_ast_create(void) {
    SECTION("AST: CREATE");
    char *s = capture_ast("CREATE TABLE users (id INT, name VARCHAR)");
    CHECK(strstr(s, "type:  CREATE") != NULL, "type CREATE");
    CHECK(strstr(s, "table: users")  != NULL, "table users");
    CHECK(strstr(s, "id INT")        != NULL, "col id INT");
    CHECK(strstr(s, "name VARCHAR")  != NULL, "col name VARCHAR");
    free(s);
}

static void test_ast_select(void) {
    SECTION("AST: SELECT WHERE ORDER LIMIT");
    char *s = capture_ast(
        "SELECT id, name FROM users WHERE age > 20 ORDER BY name DESC LIMIT 5");
    CHECK(strstr(s, "type:  SELECT")        != NULL, "type SELECT");
    CHECK(strstr(s, "columns (2)")          != NULL, "2 columns");
    CHECK(strstr(s, "age > 20")             != NULL, "where rendered");
    CHECK(strstr(s, "order_by: name DESC")  != NULL, "order_by DESC");
    CHECK(strstr(s, "limit: 5")             != NULL, "limit 5");
    free(s);
}

/* AST 출력이 혼합 AND/OR 링크를 조건 순서대로 보여주는지 확인한다.
 * (석제 ast_print 채택 — 각 조건 옆에 결합자 inline) */
static void test_ast_where_mixed_links(void) {
    SECTION("AST: WHERE mixed links");
    char *s = capture_ast(
        "SELECT * FROM users WHERE age > 20 AND name = 'bob' OR city = 'Seoul'");
    CHECK(strstr(s, "• age > 20")          != NULL, "first condition");
    CHECK(strstr(s, "• AND name = bob")    != NULL, "AND rendered inline");
    CHECK(strstr(s, "• OR city = Seoul")   != NULL, "OR rendered inline");
    free(s);
}

static void test_ast_insert(void) {
    SECTION("AST: INSERT");
    char *s = capture_ast("INSERT INTO t (a, b) VALUES (1, 'x')");
    CHECK(strstr(s, "type:  INSERT") != NULL, "type INSERT");
    CHECK(strstr(s, "values (2)")    != NULL, "2 values");
    CHECK(strstr(s, "• x")           != NULL, "value x");
    free(s);
}

static void test_ast_null_safe(void) {
    SECTION("AST: NULL safe");
    print_ast(NULL, NULL);   /* must not crash */
    g_pass++;
}

/* ─── --tokens 토큰 덤프 테스트 ──────────────────────────── */

static char *capture_tokens(const char *sql_text) {
    char  *buf = NULL;
    size_t len = 0;
    FILE  *out = open_memstream(&buf, &len);
    print_tokens(out, sql_text);
    fclose(out);
    return buf;
}

static void test_tokens_basic(void) {
    SECTION("TOKENS: 기본 SELECT");
    char *s = capture_tokens("SELECT id FROM t");
    CHECK(strstr(s, "tokens (4)") != NULL, "4 tokens");
    CHECK(strstr(s, "SELECT")     != NULL, "SELECT");
    CHECK(strstr(s, "id")         != NULL, "id");
    CHECK(strstr(s, "FROM")       != NULL, "FROM");
    free(s);
}

static void test_tokens_punctuation(void) {
    SECTION("TOKENS: 괄호/콤마/세미콜론");
    char *s = capture_tokens("INSERT INTO t (a, b) VALUES (1, 2);");
    CHECK(strstr(s, "(") != NULL, "(");
    CHECK(strstr(s, ")") != NULL, ")");
    CHECK(strstr(s, ",") != NULL, ",");
    CHECK(strstr(s, ";") != NULL, ";");
    free(s);
}

static void test_tokens_null_safe(void) {
    SECTION("TOKENS: NULL safe");
    print_tokens(NULL, NULL);
    print_tokens(stderr, NULL);
    g_pass++;
}

/* ─── 토크나이저 엣지 케이스 ─────────────────────────────── */

static void test_tokens_quoted_string(void) {
    SECTION("TOKENS: 따옴표 문자열");
    char *s = capture_tokens("INSERT INTO t (a) VALUES ('hello world')");
    CHECK(strstr(s, "hello world") != NULL, "공백 있는 문자열 한 토큰");
    free(s);

    char *s2 = capture_tokens("SELECT * FROM t WHERE a = \"foo bar\"");
    CHECK(strstr(s2, "foo bar") != NULL, "double-quoted 문자열");
    free(s2);
}

static void test_tokens_empty_quoted(void) {
    SECTION("TOKENS: 빈 따옴표 문자열");
    char *s = capture_tokens("INSERT INTO t (a) VALUES ('')");
    /* INSERT INTO t ( a ) VALUES ( '' ) → 10 tokens (빈 string 도 1개) */
    CHECK(strstr(s, "tokens (10)") != NULL, "빈 string 도 토큰 1개");
    free(s);
}

static void test_tokens_date_string(void) {
    SECTION("TOKENS: DATE 문자열 (YYYY-MM-DD)");
    char *s = capture_tokens("INSERT INTO t (d) VALUES ('2024-01-15')");
    CHECK(strstr(s, "2024-01-15") != NULL, "date 한 토큰");
    free(s);
}

static void test_tokens_compound_operators(void) {
    SECTION("TOKENS: 복합 비교 연산자");
    const char *cases[] = {
        "SELECT * FROM t WHERE a >= 1",
        "SELECT * FROM t WHERE a <= 1",
        "SELECT * FROM t WHERE a != 1",
    };
    const char *expected[] = {">=", "<=", "!="};
    for (int i = 0; i < 3; i++) {
        char *s = capture_tokens(cases[i]);
        CHECK(strstr(s, expected[i]) != NULL, expected[i]);
        free(s);
    }
}

static void test_tokens_negative_number(void) {
    SECTION("TOKENS: 음수");
    char *s = capture_tokens("SELECT * FROM t WHERE a = -5");
    CHECK(strstr(s, "-5") != NULL, "음수 -5 한 토큰");
    free(s);
}

static void test_tokens_float_number(void) {
    SECTION("TOKENS: float");
    char *s = capture_tokens("INSERT INTO t (a) VALUES (3.14)");
    CHECK(strstr(s, "3.14") != NULL, "3.14 한 토큰");
    free(s);
}

static void test_tokens_multiline(void) {
    SECTION("TOKENS: 여러 줄");
    char *s = capture_tokens(
        "SELECT id\n"
        "FROM users\n"
        "WHERE age > 20");
    CHECK(strstr(s, "tokens (8)") != NULL, "8 tokens across lines");
    free(s);
}

static void test_tokens_tabs(void) {
    SECTION("TOKENS: 탭/연속 공백");
    char *s = capture_tokens("SELECT\t\tid\t\tFROM\t\tt");
    CHECK(strstr(s, "tokens (4)") != NULL, "탭 무관 4 tokens");
    free(s);
}

static void test_tokens_comment_inline(void) {
    SECTION("TOKENS: 라인 주석 무시");
    char *s = capture_tokens(
        "-- comment line\n"
        "SELECT id FROM t -- trailing\n");
    CHECK(strstr(s, "comment") == NULL, "주석 내용 토큰화 안 됨");
    CHECK(strstr(s, "trailing") == NULL, "trailing 주석 무시");
    CHECK(strstr(s, "SELECT") != NULL, "SELECT 토큰 존재");
    free(s);
}

/* ─── JSON 출력 (print_json) 테스트 ──────────────────────── */

/* JSON 출력 결과를 문자열로 캡처해 필요한 필드가 들어 있는지 확인한다. */
static char *capture_json(const char *sql_text) {
    ParsedSQL *sql = parse_sql(sql_text);
    char  *buf = NULL;
    size_t len = 0;
    FILE  *out = open_memstream(&buf, &len);
    print_json(out, sql);
    fclose(out);
    free_parsed(sql);
    return buf;
}

static void test_json_create(void) {
    SECTION("JSON: CREATE");
    char *s = capture_json("CREATE TABLE users (id INT, name VARCHAR)");
    CHECK(strstr(s, "\"type\":\"CREATE\"")  != NULL, "type CREATE");
    CHECK(strstr(s, "\"table\":\"users\"")  != NULL, "table users");
    CHECK(strstr(s, "\"col_defs\":[\"id INT\",\"name VARCHAR\"]") != NULL, "col_defs array");
    free(s);
}

static void test_json_select(void) {
    SECTION("JSON: SELECT WHERE ORDER LIMIT");
    char *s = capture_json(
        "SELECT id, name FROM users WHERE age > 20 ORDER BY name DESC LIMIT 5");
    CHECK(strstr(s, "\"type\":\"SELECT\"")            != NULL, "type SELECT");
    CHECK(strstr(s, "\"columns\":[\"id\",\"name\"]") != NULL, "columns array");
    CHECK(strstr(s, "\"column\":\"age\"")            != NULL, "where col");
    CHECK(strstr(s, "\"op\":\">\"")                  != NULL, "where op");
    CHECK(strstr(s, "\"value\":\"20\"")              != NULL, "where val");
    CHECK(strstr(s, "\"order_by\":{\"column\":\"name\",\"asc\":false}") != NULL, "order_by DESC");
    CHECK(strstr(s, "\"limit\":5")                   != NULL, "limit 5");
    free(s);
}

static void test_json_insert(void) {
    SECTION("JSON: INSERT");
    char *s = capture_json("INSERT INTO t (a, b) VALUES (1, 'x')");
    CHECK(strstr(s, "\"type\":\"INSERT\"")          != NULL, "type INSERT");
    CHECK(strstr(s, "\"columns\":[\"a\",\"b\"]")    != NULL, "columns");
    CHECK(strstr(s, "\"values\":[\"1\",\"x\"]")     != NULL, "values");
    free(s);
}

static void test_json_where_and(void) {
    SECTION("JSON: WHERE AND (2 conditions)");
    char *s = capture_json("SELECT * FROM t WHERE a = 1 AND b = 2");
    CHECK(strstr(s, "\"where_links\":[\"AND\"]") != NULL, "where_links AND");
    /* json_out: 원우 채택 (else if) — where_links 와 where_logic 둘 중 하나만 출력 */
    int commas = 0;
    for (char *p = s; *p; p++) if (*p == '{') commas++;
    CHECK(commas >= 3, "outer + 2 where objects = 3 braces");  /* root + 2 conds */
    free(s);
}

static void test_where_between_basic(void) {
    SECTION("WHERE: BETWEEN 기본");
    ParsedSQL *s = parse_sql("SELECT * FROM t WHERE id BETWEEN 10 AND 20");
    CHECK(s != NULL, "parse_sql NULL");
    CHECK(s->where_count == 1, "where_count == 1");
    CHECK(strcmp(s->where[0].column, "id") == 0, "column == id");
    CHECK(strcmp(s->where[0].op, "BETWEEN") == 0, "op normalize BETWEEN");
    CHECK(strcmp(s->where[0].value, "10") == 0, "value == 10 (low)");
    CHECK(strcmp(s->where[0].value_to, "20") == 0, "value_to == 20 (high)");
    free_parsed(s);
}

static void test_where_between_with_and_clause(void) {
    SECTION("WHERE: BETWEEN ... AND 다음 조건");
    ParsedSQL *s = parse_sql(
        "SELECT * FROM t WHERE id BETWEEN 1 AND 5 AND name = 'alice'");
    CHECK(s->where_count == 2, "where_count == 2 (BETWEEN 의 AND 는 소비, 뒤 AND 는 결합자)");
    CHECK(strcmp(s->where[0].op, "BETWEEN") == 0, "w[0].op");
    CHECK(strcmp(s->where[0].value_to, "5") == 0, "w[0].value_to == 5");
    CHECK(strcmp(s->where[1].column, "name") == 0, "w[1].column");
    CHECK(strcmp(s->where[1].value, "alice") == 0, "w[1].value");
    free_parsed(s);
}

static void test_json_where_links(void) {
    SECTION("JSON: WHERE mixed links");
    char *s = capture_json("SELECT * FROM t WHERE a = 1 AND b = 2 OR c = 3");
    CHECK(strstr(s, "\"where_links\":[\"AND\",\"OR\"]") != NULL, "where_links array");
    CHECK(strstr(s, "\"where_logic\"") == NULL, "mixed links skip fallback logic");
    free(s);
}

static void test_json_escape(void) {
    SECTION("JSON: 문자열 이스케이프 (\" 와 \\)");
    /* 파서가 " 따옴표 안 내용을 그대로 가져옴.
     * 입력 'a\"b' 는 토크나이저가 ' 단위로 끊으므로 " 가 살아남음 → 이스케이프 검증 */
    ParsedSQL *sql = parse_sql("INSERT INTO t (a) VALUES ('he\"llo')");
    char *buf = NULL; size_t len = 0;
    FILE *out = open_memstream(&buf, &len);
    print_json(out, sql);
    fclose(out);
    CHECK(strstr(buf, "\\\"") != NULL, "\" escaped");
    free_parsed(sql);
    free(buf);
}

static void test_json_null_safe(void) {
    SECTION("JSON: NULL safe");
    print_json(NULL, NULL);
    g_pass++;
}

/* ─── --format SQL 정규화 직렬화 테스트 ──────────────────── */

/* format 출력 결과를 다시 문자열로 받아 round-trip 검증에 사용한다. */
static char *capture_format(const char *sql_text) {
    ParsedSQL *sql = parse_sql(sql_text);
    char  *buf = NULL;
    size_t len = 0;
    FILE  *out = open_memstream(&buf, &len);
    print_format(out, sql);
    fclose(out);
    free_parsed(sql);
    return buf;
}

static void test_format_create(void) {
    SECTION("FORMAT: CREATE");
    char *s = capture_format("create   table   USERS  ( id INT , name VARCHAR )");
    CHECK(strstr(s, "CREATE TABLE USERS (id INT, name VARCHAR);") != NULL,
          "정규화된 CREATE");
    free(s);
}

static void test_format_insert(void) {
    SECTION("FORMAT: INSERT");
    char *s = capture_format("INSERT INTO t (a, b) VALUES (1, 'hello world')");
    CHECK(strstr(s, "INSERT INTO t (a, b) VALUES (1, 'hello world');") != NULL,
          "정규화된 INSERT");
    free(s);
}

static void test_format_select_full(void) {
    SECTION("FORMAT: SELECT WHERE ORDER LIMIT");
    char *s = capture_format(
        "select id,name from users where age > 20 order by name desc limit 5");
    CHECK(strstr(s, "SELECT id, name FROM users WHERE age > 20") != NULL,
          "SELECT + WHERE 정규화");
    CHECK(strstr(s, "ORDER BY name DESC") != NULL, "ORDER BY DESC");
    CHECK(strstr(s, "LIMIT 5")            != NULL, "LIMIT");
    free(s);
}

/* format 출력이 혼합 AND/OR 순서를 그대로 되살리는지 확인한다. */
static void test_format_where_mixed_links(void) {
    SECTION("FORMAT: WHERE mixed links");
    char *s = capture_format(
        "select * from users where age > 20 and name = 'bob' or city = 'Seoul'");
    CHECK(strstr(s, "WHERE age > 20 AND name = 'bob' OR city = 'Seoul'") != NULL,
          "mixed links preserved");
    free(s);
}

static void test_format_delete(void) {
    SECTION("FORMAT: DELETE");
    char *s = capture_format("delete from t where id = 1");
    CHECK(strstr(s, "DELETE FROM t WHERE id = 1;") != NULL, "정규화된 DELETE");
    free(s);
}

static void test_format_update(void) {
    SECTION("FORMAT: UPDATE");
    char *s = capture_format("update t set name = 'bob', age = 30 where id = 1");
    CHECK(strstr(s, "UPDATE t SET name = 'bob', age = 30 WHERE id = 1;") != NULL,
          "정규화된 UPDATE");
    free(s);
}

static void test_format_mixed_where(void) {
    SECTION("FORMAT: mixed WHERE links");
    char *s = capture_format("select * from t where a = 1 and b = 2 or c = 3");
    CHECK(strstr(s, "WHERE a = 1 AND b = 2 OR c = 3") != NULL, "mixed WHERE preserved");
    free(s);
}

static void test_format_round_trip(void) {
    SECTION("FORMAT: round-trip (parse → format → parse 결과 동일)");
    const char *original = "SELECT id FROM t WHERE x > 10";
    char *formatted = capture_format(original);
    ParsedSQL *re = parse_sql(formatted);
    CHECK(re != NULL && re->type == QUERY_SELECT, "재파싱 결과 SELECT");
    CHECK(strcmp(re->table, "t") == 0, "table 일치");
    CHECK(re->where_count == 1 && strcmp(re->where[0].column, "x") == 0,
          "WHERE 일치");
    free_parsed(re);
    free(formatted);
}

static void test_format_null_safe(void) {
    SECTION("FORMAT: NULL safe");
    print_format(NULL, NULL);
    g_pass++;
}

int main(void) {
    test_create_table();
    test_create_all_types();
    test_insert();
    test_select_star();
    test_select_where_order_limit();
    test_where_and();
    test_where_nary_and();
    test_where_nary_mixed_logic();
    test_delete();
    test_update();
    test_delete_mixed_where();
    test_update_mixed_where();
    test_free_null();
    test_select_count_star();
    test_select_count_star_spaced();
    test_select_count_with_where();
    test_select_function_with_arg();
    test_select_aggregate_variants();
    test_select_stop_set_semicolon();
    test_empty_input();
    test_unknown_keyword();
    test_case_insensitive();
    test_default_limit();
    test_invalid_create_type();
    test_where_operators();
    test_trailing_whitespace();
    test_select_or_where();
    test_where_mixed_links();
    test_order_by_asc_explicit();
    test_sql_line_comment();

    test_ast_create();
    test_ast_select();
    test_ast_where_mixed_links();
    test_ast_insert();
    test_ast_null_safe();

    test_tokens_basic();
    test_tokens_punctuation();
    test_tokens_null_safe();
    test_tokens_quoted_string();
    test_tokens_empty_quoted();
    test_tokens_date_string();
    test_tokens_compound_operators();
    test_tokens_negative_number();
    test_tokens_float_number();
    test_tokens_multiline();
    test_tokens_tabs();
    test_tokens_comment_inline();

    test_json_create();
    test_json_select();
    test_json_insert();
    test_json_where_and();
    test_json_where_links();
    test_where_between_basic();
    test_where_between_with_and_clause();
    test_json_escape();
    test_json_null_safe();

    test_format_create();
    test_format_insert();
    test_format_select_full();
    test_format_where_mixed_links();
    test_format_delete();
    test_format_update();
    test_format_mixed_where();
    test_format_round_trip();
    test_format_null_safe();

    fprintf(stderr, "\n%d passed, %d failed\n", g_pass, g_fail);
    if (run_executor_tests() != 0) return 1;
    return g_fail == 0 ? 0 : 1;
}
