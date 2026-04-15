/* benchmark.c — B+ Tree 성능 측정 (규태 담당, 지용 레이아웃 개편).
 *
 * 사용법:  make bench
 *
 * 측정 항목:
 *   1. N 건 순차 INSERT
 *   2. N 건 랜덤 키 point-search
 *   3. 범위 검색 (bptree_range)
 *   4. 선형 vs B+ 트리 인덱스 비교 (자료구조 순수 레벨)
 *
 * 기본 N = 1,000,000. 환경변수 BENCH_N, BENCH_ORDER, BENCH_SEED,
 *                       BENCH_COMPARE_M 으로 변경 가능.
 *
 * SQL 레벨 수치 주입 (웹 UI /api/compare 에서 얻은 값):
 *   BENCH_SQL_INDEX_MS, BENCH_SQL_LINEAR_MS
 *   ① 카드에 함께 렌더. 미주입 시 안내 문구.
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

/* ═══ 측정 유틸 ═══════════════════════════════════════════════ */

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

    double insert_ms;
    int    verify_ok;

    double search_ms;
    int    search_found;

    double range_ms;
    int    range_queries;
    int    range_found;

    /* 선형 vs 인덱스 (in-memory, ms 단위 기록) */
    double linear_ms;
    double index_ms;
    int    linear_hits;
    int    index_hits;
    int    compare_m;
} Metrics;

static double clock_ms(clock_t t0, clock_t t1) {
    return (double)(t1 - t0) / (double)CLOCKS_PER_SEC * 1000.0;
}

static void do_insert(BPTree *t, const int *keys, Metrics *m) {
    clock_t t0 = clock();
    for (int i = 0; i < m->n; i++) bptree_insert(t, keys[i], i);
    clock_t t1 = clock();
    m->insert_ms = clock_ms(t0, t1);
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
    clock_t t0 = clock();
    for (int i = 0; i < m->n; i++) {
        volatile int r = bptree_search(t, keys[i]);  /* 최적화 방지 */
        if (r >= 0) found++;
    }
    clock_t t1 = clock();
    m->search_ms = clock_ms(t0, t1);
    m->search_found = found;
}

static void do_range(BPTree *t, Metrics *m) {
    int buf_size = 1000;
    int *buf = malloc(sizeof(int) * (size_t)buf_size);
    int queries = 1000;
    int total_found = 0;
    clock_t t0 = clock();
    for (int q = 0; q < queries; q++) {
        int lo = rand() % m->n + 1;
        int hi = lo + 99;
        if (hi > m->n) hi = m->n;
        total_found += bptree_range(t, lo, hi, buf, buf_size);
    }
    clock_t t1 = clock();
    m->range_ms = clock_ms(t0, t1);
    m->range_queries = queries;
    m->range_found = total_found;
    free(buf);
}

/* 선형 flat array vs B+ tree — volatile 로 최적화 제거.
 * 버그 1 수정: 이전엔 clock_gettime 을 사용했으나 컴파일러/플랫폼에 따라
 * 결과 변수가 스코프 밖으로 새는 인상이 있었다. clock() 단일 경로로 통일
 * 하고 결과를 volatile 로 받아 분명히 소비. */
static void do_compare(BPTree *t, const int *keys, Metrics *m) {
    int cm = 1000;
    const char *env = getenv("BENCH_COMPARE_M");
    if (env) { int v = atoi(env); if (v > 0) cm = v; }

    int *flat_ids = malloc(sizeof(int) * (size_t)m->n);
    int *probes   = malloc(sizeof(int) * (size_t)cm);
    if (!flat_ids || !probes) { free(flat_ids); free(probes); return; }
    for (int i = 0; i < m->n; i++) flat_ids[i] = keys[i];
    for (int i = 0; i < cm; i++) probes[i] = keys[rand() % m->n];

    /* (1) 선형 */
    int lh = 0;
    clock_t t0 = clock();
    for (int q = 0; q < cm; q++) {
        int target = probes[q];
        volatile int found_at = -1;
        for (int i = 0; i < m->n; i++) {
            if (flat_ids[i] == target) { found_at = i; break; }
        }
        if (found_at >= 0) lh++;
    }
    clock_t t1 = clock();
    m->linear_ms = clock_ms(t0, t1);

    /* (2) B+ 트리 */
    int ih = 0;
    t0 = clock();
    for (int q = 0; q < cm; q++) {
        volatile int r = bptree_search(t, probes[q]);
        if (r >= 0) ih++;
    }
    t1 = clock();
    m->index_ms = clock_ms(t0, t1);

    m->linear_hits = lh;
    m->index_hits  = ih;
    m->compare_m   = cm;
    free(flat_ids); free(probes);
}

/* ═══ 3-col 레이아웃 ═══════════════════════════════════════════ */

#define MAX_LINES 64
#define MAX_LINE_BYTES 512
static char g_col[3][MAX_LINES][MAX_LINE_BYTES];
static int  g_lines[3] = {0, 0, 0};

static void add_line(int c, const char *fmt, ...) {
    if (g_lines[c] >= MAX_LINES) return;
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(g_col[c][g_lines[c]], MAX_LINE_BYTES, fmt, ap);
    va_end(ap);
    g_lines[c]++;
}

static int get_term_width(void) {
    struct winsize ws;
    if (isatty(STDOUT_FILENO) &&
        ioctl(STDOUT_FILENO, TIOCGWINSZ, &ws) == 0 &&
        ws.ws_col > 60) {
        return ws.ws_col;
    }
    return 120;
}

/* UTF-8 + ANSI 제외한 실제 화면 표시 너비. */
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

static void print_padded(const char *s, int width) {
    fputs(s, stdout);
    int vw = visible_width(s);
    for (int i = vw; i < width; i++) putchar(' ');
}

/* col_w 내에서 n 개 \u2588 를 색과 함께 문자열로 만들기.
 * %.*s 는 byte 기준이라 UTF-8 멀티바이트에서 깨지므로 직접 반복. */
static void make_bar(char *dst, size_t dstsz, const char *color, int n, const char *suffix_dim) {
    int off = 0;
    off += snprintf(dst + off, dstsz - (size_t)off, "%s", color);
    for (int i = 0; i < n && off + 4 < (int)dstsz; i++) {
        off += snprintf(dst + off, dstsz - (size_t)off, "\u2588");
    }
    off += snprintf(dst + off, dstsz - (size_t)off, "%s", C_RESET);
    if (suffix_dim) {
        off += snprintf(dst + off, dstsz - (size_t)off, "%s%s%s",
                         C_DIM, suffix_dim, C_RESET);
    }
    dst[dstsz - 1] = '\0';
}

static int compute_bar_len(double value, double max_v, int bar_max) {
    if (max_v <= 0 || bar_max < 1) return 0;
    int n = (int)(value / max_v * bar_max);
    if (n < 1 && value > 0) n = 1;
    if (n > bar_max) n = bar_max;
    return n;
}

/* ═══ 카드 빌더 ═══════════════════════════════════════════════ */

static void build_sql_card(int col, int col_w,
                            double sql_idx_ms, double sql_lin_ms,
                            int near_zero) {
    int bar_max = col_w - 14;
    if (bar_max < 10) bar_max = 10;

    add_line(col, C_BOLD "\u2460 SQL END-TO-END" C_RESET);
    add_line(col, C_DIM "/api/compare \u00B7 subprocess" C_RESET);
    add_line(col, C_DIM "ensure_index rebuild 포함" C_RESET);
    add_line(col, "");

    if (sql_idx_ms > 0 && sql_lin_ms > 0) {
        double ratio = sql_lin_ms / sql_idx_ms;
        int blin = compute_bar_len(sql_lin_ms, sql_lin_ms, bar_max);
        int bidx = compute_bar_len(sql_idx_ms, sql_lin_ms, bar_max);
        char bar1[512], bar2[512];
        char buf1[64], buf2[64];
        snprintf(buf1, sizeof(buf1), "  %.1fms", sql_lin_ms);
        snprintf(buf2, sizeof(buf2), "  %.1fms", sql_idx_ms);
        make_bar(bar1, sizeof(bar1), C_RED,   blin, buf1);
        make_bar(bar2, sizeof(bar2), C_WHITE, bidx, buf2);

        add_line(col, "  " C_RED "선형 (status)" C_RESET);
        add_line(col, "  %s", bar1);
        add_line(col, "  " C_WHITE "인덱스 (BETWEEN)" C_RESET);
        add_line(col, "  %s", bar2);
        add_line(col, "");
        if (near_zero) {
            add_line(col, "  " C_BRED "측정불가 (< 1\u03BCs)" C_RESET);
        } else {
            add_line(col, "  " C_BRED "%.1f\u00D7" C_RESET " 단축", ratio);
        }
        add_line(col, "  " C_DIM "인덱스 %.2fms \u00B7 선형 %.2fms" C_RESET,
                 sql_idx_ms, sql_lin_ms);
    } else {
        add_line(col, "  " C_DIM "(측정 값 미주입)" C_RESET);
        add_line(col, "");
        add_line(col, "  " C_REV " BENCH_SQL_INDEX_MS " C_RESET);
        add_line(col, "  " C_REV " BENCH_SQL_LINEAR_MS " C_RESET);
        add_line(col, "  " C_DIM "환경변수로 주입하거나" C_RESET);
        add_line(col, "  " C_DIM "웹 UI /api/compare" C_RESET);
        add_line(col, "  " C_DIM "결과를 복사해서 사용." C_RESET);
    }
}

static void build_bench_card(int col, int col_w, const Metrics *m) {
    int bar_max = col_w - 14;
    if (bar_max < 10) bar_max = 10;

    add_line(col, C_BOLD "\u2461 자료구조 순수" C_RESET);
    add_line(col, C_DIM "make bench \u00B7 bptree_search" C_RESET);
    add_line(col, C_DIM "인-프로세스 호출만" C_RESET);
    add_line(col, "");

    /* M 회 평균 query 당 시간 (per_query) */
    double per_index = m->compare_m > 0 ? m->index_ms  / m->compare_m : 0.0;
    double per_linear = m->compare_m > 0 ? m->linear_ms / m->compare_m : 0.0;
    int near_zero = (m->index_ms < 0.001);  /* 0.001 ms 미만 = 1μs 미만 */

    int blin = compute_bar_len(m->linear_ms, m->linear_ms, bar_max);
    int bidx = compute_bar_len(m->index_ms,  m->linear_ms, bar_max);

    char bar1[512], bar2[512];
    char buf1[64], buf2[64];
    snprintf(buf1, sizeof(buf1), "  %.2fms", m->linear_ms);
    snprintf(buf2, sizeof(buf2), near_zero ? "  < 0.01ms" : "  %.2fms", m->index_ms);
    make_bar(bar1, sizeof(bar1), C_RED,   blin, buf1);
    make_bar(bar2, sizeof(bar2), C_WHITE, bidx, buf2);

    add_line(col, "  " C_RED "선형 flat array" C_RESET);
    add_line(col, "  %s", bar1);
    add_line(col, "  " C_WHITE "bptree_search" C_RESET);
    add_line(col, "  %s", bar2);
    add_line(col, "");

    if (near_zero) {
        add_line(col, "  " C_BRED "측정불가 (< 1\u03BCs)" C_RESET);
        add_line(col, "  " C_DIM "M=%d 회는 index 가 너무 빨라 clock() 해상도 미만."
                 C_RESET, m->compare_m);
    } else {
        double ratio = m->linear_ms / m->index_ms;
        add_line(col, "  " C_BRED "%.0f\u00D7" C_RESET " 단축", ratio);
    }
    add_line(col, "  " C_DIM "per query: %.4fms \u00B7 %.4fms" C_RESET,
             per_index, per_linear);
    add_line(col, "  " C_DIM "N=%d \u00B7 M=%d" C_RESET, m->n, m->compare_m);
}

static void build_why_card(int col) {
    add_line(col, C_BRED "\u2462 두 배율이 다른 이유" C_RESET);
    add_line(col, C_DIM "\u2460 에 포함된 고정비:" C_RESET);
    add_line(col, "");
    add_line(col, "\u2022 " C_REV " subprocess " C_RESET);
    add_line(col, "  fork + exec");
    add_line(col, "\u2022 " C_REV " SQL 파싱 " C_RESET);
    add_line(col, "  tokenize + AST");
    add_line(col, "\u2022 " C_REV " ensure_index rebuild " C_RESET);
    add_line(col, "  ~1.8s / call");
    add_line(col, "\u2022 " C_REV " 파일 I/O " C_RESET);
    add_line(col, "  CSV / BIN 읽기");
    add_line(col, "");
    add_line(col, C_DIM "PostgreSQL 같은" C_RESET);
    add_line(col, C_DIM "영속 데몬이면" C_RESET);
    add_line(col, C_DIM "rebuild 가 사라져" C_RESET);
    add_line(col, C_DIM "\u2460 이 \u2461 에 수렴." C_RESET);
}

/* ═══ 출력 ═══════════════════════════════════════════════════ */

static void hr(int total_w, int heavy) {
    for (int i = 0; i < total_w; i++) fputs(heavy ? "\u2501" : "\u2500", stdout);
    putchar('\n');
}

static void print_centered(int total_w, const char *s, const char *color) {
    int w = visible_width(s);
    int pad = (total_w - w) / 2;
    if (pad < 0) pad = 0;
    for (int i = 0; i < pad; i++) putchar(' ');
    printf("%s%s%s\n", color, s, C_RESET);
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

    /* 측정 — 기존 로직 그대로 (bptree_*, clock) */
    do_insert(tree, keys, &m);
    do_verify(tree, keys, &m);
    shuffle(keys, m.n);
    do_search(tree, keys, &m);
    do_range(tree, &m);
    do_compare(tree, keys, &m);

    /* 터미널 너비 + 3-col 분할 */
    int term_w = get_term_width();
    if (term_w < 80) term_w = 80;
    int col_w = (term_w - 4) / 3;  /* " │ " 2개 = 6바이트, 표시폭 4 */
    if (col_w < 26) col_w = 26;

    /* SQL 주입값 */
    const char *env_sql_idx = getenv("BENCH_SQL_INDEX_MS");
    const char *env_sql_lin = getenv("BENCH_SQL_LINEAR_MS");
    double sql_idx = env_sql_idx ? atof(env_sql_idx) : 0;
    double sql_lin = env_sql_lin ? atof(env_sql_lin) : 0;

    /* ═══ 헤더 ═══ */
    hr(term_w, 1);
    print_centered(term_w, "B+ TREE BENCHMARK  \u00B7  선형 vs 인덱스", C_BOLD);
    {
        char sub[256];
        snprintf(sub, sizeof(sub),
                 "N=%d  \u00B7  order=%d  \u00B7  compare M=%d  \u00B7  seed=%u",
                 m.n, m.order, m.compare_m, m.seed);
        print_centered(term_w, sub, C_DIM);
    }
    hr(term_w, 0);

    /* 카드 내용 채우기 */
    build_sql_card(0, col_w, sql_idx, sql_lin, 0);
    build_bench_card(1, col_w, &m);
    build_why_card(2);

    int max_lines = g_lines[0];
    if (g_lines[1] > max_lines) max_lines = g_lines[1];
    if (g_lines[2] > max_lines) max_lines = g_lines[2];

    /* 병렬 출력 — %-*s 는 byte 단위라 CJK/ANSI 에서 오정렬.
     * print_padded 가 visible_width 기반으로 공백 패딩해 정확히 맞춤. */
    for (int i = 0; i < max_lines; i++) {
        print_padded(i < g_lines[0] ? g_col[0][i] : "", col_w);
        printf(" " C_DIM "\u2502" C_RESET " ");
        print_padded(i < g_lines[1] ? g_col[1][i] : "", col_w);
        printf(" " C_DIM "\u2502" C_RESET " ");
        print_padded(i < g_lines[2] ? g_col[2][i] : "", col_w);
        putchar('\n');
    }

    hr(term_w, 0);

    /* 푸터 한 줄 — INSERT / SEARCH / RANGE / VERIFY */
    {
        double insert_ops = m.insert_ms > 0 ? m.n / (m.insert_ms / 1000.0) : 0;
        double search_ops = m.search_ms > 0 ? m.n / (m.search_ms / 1000.0) : 0;
        double range_qps  = m.range_ms  > 0 ? m.range_queries / (m.range_ms / 1000.0) : 0;

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
        int pad = (term_w - visible_width(line)) / 2;
        if (pad < 0) pad = 0;
        for (int i = 0; i < pad; i++) putchar(' ');
        fputs(line, stdout);
        putchar('\n');
    }
    hr(term_w, 1);

    bptree_destroy(tree);
    free(keys);
    return 0;
}
