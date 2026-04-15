/* benchmark.c — B+ Tree INSERT / SELECT 성능 측정 (규태 담당).
 *
 * 사용법:  make bench
 *
 * 측정 항목:
 *   1. N 건 순차 INSERT 소요 시간
 *   2. N 건 랜덤 키 point-search (bptree_search) 소요 시간
 *   3. 범위 검색 (bptree_range) 소요 시간
 *   4. 선형 vs B+ 트리 인덱스 비교
 *
 * 기본 N = 1,000,000. 환경변수 BENCH_N, BENCH_ORDER, BENCH_SEED,
 * BENCH_COMPARE_M 으로 변경 가능.
 *
 * SQL 레벨 수치 주입 (웹 UI /api/compare 에서 얻은 값):
 *   BENCH_SQL_INDEX_MS, BENCH_SQL_LINEAR_MS  → 3-col 레이아웃의 ① 카드에 표시.
 *   미설정 시 ① 카드는 안내 문구로 대체.
 */

#include "bptree.h"

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <time.h>
#include <unistd.h>

/* ═══ ANSI SGR ════════════════════════════════════════════════ */
#define C_RESET   "\033[0m"
#define C_BOLD    "\033[1m"
#define C_DIM     "\033[2m"
#define C_REV     "\033[7m"
#define C_RED     "\033[31m"
#define C_WHITE   "\033[37m"
#define C_BRED    "\033[1;31m"
#define C_BWHITE  "\033[1;37m"

/* ═══ 측정 유틸 ═══════════════════════════════════════════════ */

static double now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}

static void shuffle(int *arr, int n) {
    for (int i = n - 1; i > 0; i--) {
        int j = rand() % (i + 1);
        int tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }
}

static int *gen_keys(int n) {
    int *keys = malloc(sizeof(int) * (size_t)n);
    if (!keys) {
        fprintf(stderr, "[bench] malloc failed (n=%d)\n", n);
        exit(1);
    }
    for (int i = 0; i < n; i++) keys[i] = i + 1;
    shuffle(keys, n);
    return keys;
}

typedef struct {
    int    n, order;
    unsigned seed;

    double insert_sec;
    int    verify_ok;

    double search_sec;
    int    search_found;

    double range_sec;
    int    range_queries;
    int    range_found;

    /* 선형 vs 인덱스 (in-memory) */
    double linear_sec;
    double index_sec;
    int    linear_hits;
    int    index_hits;
    int    compare_m;
} Metrics;

static void do_insert(BPTree *t, const int *keys, Metrics *m) {
    double t0 = now_sec();
    for (int i = 0; i < m->n; i++) bptree_insert(t, keys[i], i);
    m->insert_sec = now_sec() - t0;
}

static void do_verify(BPTree *t, const int *keys, Metrics *m) {
    int ok = 0;
    for (int i = 0; i < m->n; i++) {
        if (bptree_search(t, keys[i]) >= 0) ok++;
    }
    m->verify_ok = ok;
}

static void do_search(BPTree *t, const int *keys, Metrics *m) {
    int found = 0;
    double t0 = now_sec();
    for (int i = 0; i < m->n; i++) {
        if (bptree_search(t, keys[i]) >= 0) found++;
    }
    m->search_sec = now_sec() - t0;
    m->search_found = found;
}

static void do_range(BPTree *t, Metrics *m) {
    int buf_size = 1000;
    int *buf = malloc(sizeof(int) * (size_t)buf_size);
    int queries = 1000;
    int total_found = 0;
    double t0 = now_sec();
    for (int q = 0; q < queries; q++) {
        int lo = rand() % m->n + 1;
        int hi = lo + 99;
        if (hi > m->n) hi = m->n;
        total_found += bptree_range(t, lo, hi, buf, buf_size);
    }
    m->range_sec = now_sec() - t0;
    m->range_queries = queries;
    m->range_found = total_found;
    free(buf);
}

static void do_compare(BPTree *t, const int *keys, Metrics *m) {
    int cm = 1000;
    const char *env = getenv("BENCH_COMPARE_M");
    if (env) { int v = atoi(env); if (v > 0) cm = v; }

    int *flat_ids = malloc(sizeof(int) * (size_t)m->n);
    int *probes   = malloc(sizeof(int) * (size_t)cm);
    if (!flat_ids || !probes) { free(flat_ids); free(probes); return; }
    for (int i = 0; i < m->n; i++) flat_ids[i] = keys[i];
    for (int i = 0; i < cm; i++) probes[i] = keys[rand() % m->n];

    int lh = 0;
    double t0 = now_sec();
    for (int q = 0; q < cm; q++) {
        int target = probes[q];
        for (int i = 0; i < m->n; i++) if (flat_ids[i] == target) { lh++; break; }
    }
    m->linear_sec = now_sec() - t0;

    int ih = 0;
    t0 = now_sec();
    for (int q = 0; q < cm; q++) if (bptree_search(t, probes[q]) >= 0) ih++;
    m->index_sec = now_sec() - t0;

    m->linear_hits = lh;
    m->index_hits  = ih;
    m->compare_m   = cm;
    free(flat_ids); free(probes);
}

/* ═══ 3-col 터미널 레이아웃 ══════════════════════════════════ */

/* 실제 터미널 너비 (ANSI/CJK 고려). isatty 아니면 120. */
static int get_term_width(void) {
    struct winsize ws;
    if (isatty(STDOUT_FILENO) &&
        ioctl(STDOUT_FILENO, TIOCGWINSZ, &ws) == 0 &&
        ws.ws_col > 40) {
        return ws.ws_col;
    }
    return 120;
}

/* UTF-8 + ANSI 를 제외한 화면 표시 너비 계산.
 * ASCII 1, 2-byte UTF-8 1, 3/4-byte (대개 CJK/이모지) 2 로 간주. */
static int visible_width(const char *s) {
    int w = 0;
    const unsigned char *p = (const unsigned char *)s;
    while (*p) {
        if (*p == 0x1b) {
            while (*p && *p != 'm') p++;
            if (*p) p++;
            continue;
        }
        unsigned char c = *p;
        if (c < 0x80)                  { w += 1; p += 1; }
        else if ((c & 0xE0) == 0xC0)   { w += 1; p += 2; }
        else if ((c & 0xF0) == 0xE0)   { w += 2; p += 3; }
        else if ((c & 0xF8) == 0xF0)   { w += 2; p += 4; }
        else                            { p += 1; }
    }
    return w;
}

typedef struct {
    char **lines;
    int    count, cap;
    int    width;
} Col;

static void col_init(Col *c, int width) {
    c->lines = NULL; c->count = 0; c->cap = 0; c->width = width;
}

static void col_push(Col *c, const char *s) {
    if (c->count >= c->cap) {
        c->cap = c->cap ? c->cap * 2 : 16;
        c->lines = realloc(c->lines, sizeof(char *) * (size_t)c->cap);
    }
    c->lines[c->count++] = strdup(s);
}

static void col_pushf(Col *c, const char *fmt, ...) {
    char buf[1024];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    col_push(c, buf);
}

/* 긴 텍스트를 col.width 에 맞춰 문자 단위로 wrap. ANSI escape 는 보존. */
static void col_pushwrap(Col *c, const char *text) {
    const char *p = text;
    int width = c->width;
    if (width < 10) width = 10;
    while (*p) {
        char line[2048];
        int llen = 0, lw = 0;
        while (*p && lw < width && llen < (int)sizeof(line) - 8) {
            unsigned char b = (unsigned char)*p;
            if (b == 0x1b) {
                while (*p && *p != 'm' && llen < (int)sizeof(line) - 2) line[llen++] = *p++;
                if (*p == 'm') line[llen++] = *p++;
                continue;
            }
            int bytes, cw;
            if (b < 0x80)                 { bytes = 1; cw = 1; }
            else if ((b & 0xE0) == 0xC0)  { bytes = 2; cw = 1; }
            else if ((b & 0xF0) == 0xE0)  { bytes = 3; cw = 2; }
            else                           { bytes = 4; cw = 2; }
            if (lw + cw > width) break;
            for (int i = 0; i < bytes && *p; i++) line[llen++] = *p++;
            lw += cw;
        }
        line[llen] = '\0';
        col_push(c, line);
        /* 다음 줄 선두 공백 제거 */
        while (*p == ' ') p++;
    }
}

/* 막대 차트 한 쌍 (라벨 줄 + 바 줄). max_v 에 비례해 col_w - 24 의 █. */
static void col_bar(Col *c, const char *label, double value, double max_v,
                    const char *color) {
    int max_bar = c->width - 22;
    if (max_bar < 8) max_bar = 8;
    int len = (max_v > 0) ? (int)(value / max_v * max_bar) : 0;
    if (len < 1 && value > 0) len = 1;
    if (len > max_bar) len = max_bar;

    col_pushf(c, "  %s", label);

    char line[2048];
    int n = 0;
    n += snprintf(line + n, sizeof(line) - (size_t)n, "  %s", color);
    for (int i = 0; i < len; i++)
        n += snprintf(line + n, sizeof(line) - (size_t)n, "\u2588");
    n += snprintf(line + n, sizeof(line) - (size_t)n, "%s", C_RESET);

    char valbuf[64];
    if (value >= 0.01)
        snprintf(valbuf, sizeof(valbuf), "  %.3fs", value);
    else
        snprintf(valbuf, sizeof(valbuf), "  %.2fms", value * 1000.0);
    snprintf(line + n, sizeof(line) - (size_t)n, "%s%s%s", C_DIM, valbuf, C_RESET);
    col_push(c, line);
}

static void col_free(Col *c) {
    for (int i = 0; i < c->count; i++) free(c->lines[i]);
    free(c->lines);
}

static void col_print_line(const Col *c, int idx) {
    const char *line = (idx < c->count) ? c->lines[idx] : "";
    int vw = visible_width(line);
    fputs(line, stdout);
    for (int i = vw; i < c->width; i++) putchar(' ');
}

static void hr(int total_w, char ch) {
    for (int i = 0; i < total_w; i++) {
        if (ch == '-')  fputs("\u2500", stdout);
        else if (ch == '=') fputs("\u2501", stdout);
        else putchar(ch);
    }
    putchar('\n');
}

static void print_centered_bold(int total_w, const char *s) {
    int w = visible_width(s);
    int pad = (total_w - w) / 2;
    if (pad < 0) pad = 0;
    for (int i = 0; i < pad; i++) putchar(' ');
    printf(C_BOLD "%s" C_RESET "\n", s);
}

static void print_centered_dim(int total_w, const char *s) {
    int w = visible_width(s);
    int pad = (total_w - w) / 2;
    if (pad < 0) pad = 0;
    for (int i = 0; i < pad; i++) putchar(' ');
    printf(C_DIM "%s" C_RESET "\n", s);
}

/* ═══ 카드 컨텐츠 빌더 ═══════════════════════════════════════ */

static void build_col_sql(Col *c, const Metrics *m,
                          double sql_index_ms, double sql_linear_ms) {
    col_push(c, C_BOLD "\u2460 SQL END-TO-END" C_RESET);
    col_pushwrap(c, C_DIM "subprocess + SQL 파싱 + ensure_index rebuild + 파일 I/O 포함. 웹 /api/compare 와 동일 경로." C_RESET);
    col_push(c, "");

    if (sql_index_ms > 0 && sql_linear_ms > 0) {
        double maxv = sql_linear_ms / 1000.0;
        col_bar(c, C_RED "선형 (status)" C_RESET,
                sql_linear_ms / 1000.0, maxv, C_RED);
        col_bar(c, C_WHITE "인덱스 (BETWEEN)" C_RESET,
                sql_index_ms / 1000.0, maxv, C_WHITE);
        col_push(c, "");
        double sp = sql_linear_ms / sql_index_ms;
        col_pushf(c, "  " C_BRED "%.1f\u00D7" C_RESET " 단축", sp);
        col_pushf(c, "  " C_DIM "인덱스 %.0fms \u00B7 선형 %.0fms" C_RESET,
                  sql_index_ms, sql_linear_ms);
    } else {
        col_push(c, "  " C_DIM "(측정 값 미주입)" C_RESET);
        col_push(c, "");
        col_pushwrap(c, C_DIM "환경변수 " C_RESET C_REV " BENCH_SQL_INDEX_MS " C_RESET
                       C_DIM " 과 " C_RESET C_REV " BENCH_SQL_LINEAR_MS " C_RESET
                       C_DIM " 로 주입하세요." C_RESET);
        col_push(c, "");
        col_pushwrap(c, C_DIM "또는 웹 UI /api/compare 결과의 index_ms / linear_ms 를 그대로 복사." C_RESET);
    }
    (void)m;
}

static void build_col_bench(Col *c, const Metrics *m) {
    col_push(c, C_BOLD "\u2461 자료구조 순수" C_RESET);
    col_pushwrap(c, C_DIM "bptree_search 인-프로세스 호출만. subprocess / 파일 / SQL 파싱 없음." C_RESET);
    col_push(c, "");

    double maxv = m->linear_sec;
    col_bar(c, C_RED "선형 flat array" C_RESET, m->linear_sec, maxv, C_RED);
    col_bar(c, C_WHITE "bptree_search" C_RESET,  m->index_sec,  maxv, C_WHITE);
    col_push(c, "");
    double sp = (m->index_sec > 0) ? m->linear_sec / m->index_sec : 0;
    col_pushf(c, "  " C_BRED "%.0f\u00D7" C_RESET " 단축", sp);
    col_pushf(c, "  " C_DIM "인덱스 %.3fs \u00B7 선형 %.3fs" C_RESET,
              m->index_sec, m->linear_sec);
    col_pushf(c, "  " C_DIM "N=%d \u00B7 M=%d" C_RESET, m->n, m->compare_m);
}

static void build_col_why(Col *c) {
    col_push(c, C_BRED "\u2462 두 배율이 다른 이유" C_RESET);
    col_pushwrap(c, C_DIM "\u2460 에는 \u2461 에 없는 고정비가 포함됩니다:" C_RESET);
    col_push(c, "");
    col_pushwrap(c, "\u2022 " C_REV " subprocess " C_RESET "  fork + exec");
    col_pushwrap(c, "\u2022 " C_REV " SQL 파싱 " C_RESET "  tokenize + AST");
    col_pushwrap(c, "\u2022 " C_REV " ensure_index rebuild " C_RESET "  ~1.8s / call");
    col_pushwrap(c, "\u2022 " C_REV " 파일 I/O " C_RESET "  CSV / BIN 읽기");
    col_push(c, "");
    col_pushwrap(c, C_DIM "PostgreSQL 같은 영속 데몬이면 rebuild 가 사라져 \u2460 이 \u2461 에 수렴." C_RESET);
}

/* ═══ main ═══════════════════════════════════════════════════ */

int main(void) {
    Metrics m = {0};

    m.n = 1000000;
    const char *env = getenv("BENCH_N");
    if (env) { int v = atoi(env); if (v > 0) m.n = v; }

    m.order = 128;
    const char *env_order = getenv("BENCH_ORDER");
    if (env_order) { int v = atoi(env_order); if (v >= 3) m.order = v; }

    m.seed = (unsigned)time(NULL);
    const char *env_seed = getenv("BENCH_SEED");
    if (env_seed) m.seed = (unsigned)atoi(env_seed);
    srand(m.seed);

    int *keys = gen_keys(m.n);
    BPTree *tree = bptree_create(m.order);
    if (!tree) { fprintf(stderr, "[bench] bptree_create failed\n"); free(keys); return 1; }

    /* 측정 — 기존 로직 그대로 */
    do_insert(tree, keys, &m);
    do_verify(tree, keys, &m);
    shuffle(keys, m.n);
    do_search(tree, keys, &m);
    do_range(tree, &m);
    do_compare(tree, keys, &m);

    /* 터미널 레이아웃 */
    int tw = get_term_width();
    if (tw < 80) tw = 80;
    if (tw > 200) tw = 200;  /* 너무 넓으면 읽기 부담 */

    const char *env_sql_idx = getenv("BENCH_SQL_INDEX_MS");
    const char *env_sql_lin = getenv("BENCH_SQL_LINEAR_MS");
    double sql_idx = env_sql_idx ? atof(env_sql_idx) : 0;
    double sql_lin = env_sql_lin ? atof(env_sql_lin) : 0;

    /* ═══ 헤더 ═══ */
    hr(tw, '=');
    print_centered_bold(tw, "B+ TREE BENCHMARK  \u00B7  선형 vs 인덱스");
    {
        char sub[256];
        snprintf(sub, sizeof(sub),
                 "N=%d  \u00B7  order=%d  \u00B7  compare M=%d  \u00B7  seed=%u",
                 m.n, m.order, m.compare_m, m.seed);
        print_centered_dim(tw, sub);
    }
    hr(tw, '-');

    /* ═══ 3-col 영역 ═══ */
    int gap = 3;  /* " \u2502 " = 공백 + 선 + 공백 (화면 3칸) */
    int col_w = (tw - 2 * gap) / 3;
    if (col_w < 24) col_w = 24;

    Col c1, c2, c3;
    col_init(&c1, col_w);
    col_init(&c2, col_w);
    col_init(&c3, col_w);

    build_col_sql(&c1, &m, sql_idx, sql_lin);
    build_col_bench(&c2, &m);
    build_col_why(&c3);

    int rows = c1.count;
    if (c2.count > rows) rows = c2.count;
    if (c3.count > rows) rows = c3.count;

    for (int r = 0; r < rows; r++) {
        col_print_line(&c1, r);
        printf(" " C_DIM "\u2502" C_RESET " ");
        col_print_line(&c2, r);
        printf(" " C_DIM "\u2502" C_RESET " ");
        col_print_line(&c3, r);
        putchar('\n');
    }

    hr(tw, '-');

    /* ═══ 총평 (단일 행) ═══ */
    {
        double insert_ops = m.insert_sec > 0 ? m.n / m.insert_sec : 0;
        double search_ops = m.search_sec > 0 ? m.n / m.search_sec : 0;
        double range_qps  = m.range_sec  > 0 ? m.range_queries / m.range_sec : 0;

        char line[512];
        snprintf(line, sizeof(line),
                 C_BOLD "INSERT " C_RESET "%s%.2fM ops/s" C_RESET
                 "  \u00B7  " C_BOLD "SEARCH " C_RESET "%s%.2fM ops/s" C_RESET
                 "  \u00B7  " C_BOLD "RANGE " C_RESET "%s%.2fM qps" C_RESET
                 "  \u00B7  " C_BOLD "VERIFY " C_RESET "%d/%d",
                 C_WHITE, insert_ops / 1e6,
                 C_WHITE, search_ops / 1e6,
                 C_WHITE, range_qps / 1e6,
                 m.verify_ok, m.n);
        int pad = (tw - visible_width(line)) / 2;
        if (pad < 0) pad = 0;
        for (int i = 0; i < pad; i++) putchar(' ');
        fputs(line, stdout);
        putchar('\n');
    }
    hr(tw, '=');

    col_free(&c1); col_free(&c2); col_free(&c3);
    bptree_destroy(tree);
    free(keys);
    return 0;
}
