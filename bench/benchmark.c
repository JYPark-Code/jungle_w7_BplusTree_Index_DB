/* benchmark.c — B+ Tree INSERT / SELECT 성능 측정 (규태 담당).
 *
 * 사용법:  make bench
 *
 * 측정 항목:
 *   1. N 건 순차 INSERT 소요 시간
 *   2. N 건 랜덤 키 point-search (bptree_search) 소요 시간
 *   3. 범위 검색 (bptree_range) 소요 시간
 *
 * 기본 N = 1,000,000. 환경변수 BENCH_N 으로 변경 가능.
 */

#include "bptree.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* ── 유틸 ─────────────────────────────────────────────────────── */

static double now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}

/* Fisher-Yates 셔플 */
static void shuffle(int *arr, int n) {
    for (int i = n - 1; i > 0; i--) {
        int j = rand() % (i + 1);
        int tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }
}

/* ── 더미 데이터 생성 ────────────────────────────────────────── */

/* keys[0..n-1] = 1..n 을 랜덤 셔플한 배열. 호출자 free. */
static int *gen_keys(int n) {
    int *keys = malloc(sizeof(int) * (size_t)n);
    if (!keys) {
        fprintf(stderr, "[bench] malloc failed (n=%d)\n", n);
        exit(1);
    }
    for (int i = 0; i < n; i++)
        keys[i] = i + 1;
    shuffle(keys, n);
    return keys;
}

/* ── 벤치마크 러너 ───────────────────────────────────────────── */

static void bench_insert(BPTree *tree, const int *keys, int n) {
    double t0 = now_sec();
    for (int i = 0; i < n; i++)
        bptree_insert(tree, keys[i], i);
    double elapsed = now_sec() - t0;

    printf("  INSERT  %d 건  |  %.3f s  |  %.0f ops/s\n",
           n, elapsed, n / elapsed);
}

static void bench_search(BPTree *tree, const int *keys, int n) {
    int found = 0;
    double t0 = now_sec();
    for (int i = 0; i < n; i++) {
        if (bptree_search(tree, keys[i]) >= 0)
            found++;
    }
    double elapsed = now_sec() - t0;

    printf("  SEARCH  %d 건  |  %.3f s  |  %.0f ops/s  (found %d)\n",
           n, elapsed, n / elapsed, found);
}

static void bench_range(BPTree *tree, int n) {
    int buf_size = 1000;
    int *buf = malloc(sizeof(int) * (size_t)buf_size);
    if (!buf) {
        fprintf(stderr, "[bench] malloc failed\n");
        return;
    }

    int queries = 1000;
    int total_found = 0;

    double t0 = now_sec();
    for (int q = 0; q < queries; q++) {
        int lo = rand() % n + 1;
        int hi = lo + 99;
        if (hi > n) hi = n;
        int cnt = bptree_range(tree, lo, hi, buf, buf_size);
        total_found += cnt;
    }
    double elapsed = now_sec() - t0;

    printf("  RANGE   %d 회 (폭 100)  |  %.3f s  |  %.0f qps  (total hits %d)\n",
           queries, elapsed, queries / elapsed, total_found);

    free(buf);
}

/* ── main ─────────────────────────────────────────────────────── */

int main(void) {
    int n = 1000000;
    const char *env = getenv("BENCH_N");
    if (env) {
        int v = atoi(env);
        if (v > 0) n = v;
    }

    int order = 128;
    const char *env_order = getenv("BENCH_ORDER");
    if (env_order) {
        int v = atoi(env_order);
        if (v >= 3) order = v;
    }

    unsigned seed = (unsigned)time(NULL);
    const char *env_seed = getenv("BENCH_SEED");
    if (env_seed) {
        seed = (unsigned)atoi(env_seed);
    }
    srand(seed);

    printf("=== B+ Tree Benchmark ===\n");
    printf("  N = %d,  order = %d,  seed = %u\n\n", n, order, seed);

    /* 키 생성 */
    int *keys = gen_keys(n);

    /* 트리 생성 */
    BPTree *tree = bptree_create(order);
    if (!tree) {
        fprintf(stderr, "[bench] bptree_create failed\n");
        free(keys);
        return 1;
    }

    /* 1) INSERT 벤치마크 */
    bench_insert(tree, keys, n);

    /* 1.5) INSERT 정합성 검증 */
    {
        int verified = 0;
        for (int i = 0; i < n; i++) {
            if (bptree_search(tree, keys[i]) >= 0)
                verified++;
        }
        printf("  VERIFY  %d / %d 건 조회 성공 (%.1f%%)\n\n",
               verified, n, 100.0 * verified / n);
    }

    /* 2) SEARCH 벤치마크 (삽입과 다른 순서로 셔플) */
    shuffle(keys, n);
    bench_search(tree, keys, n);

    /* 3) RANGE 벤치마크 */
    bench_range(tree, n);

    printf("\n=== Done ===\n");

    /* 정리 */
    bptree_destroy(tree);
    free(keys);
    return 0;
}
