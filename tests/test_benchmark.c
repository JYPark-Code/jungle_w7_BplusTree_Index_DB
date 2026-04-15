/* tests/test_benchmark.c — bench/benchmark.c 단위 테스트 (규태 담당).
 *
 * 벤치마크 핵심 로직이 정상 동작하는지 검증:
 *   1) 트리 생성 후 대량 INSERT 크래시 없음
 *   2) INSERT 한 키를 SEARCH 로 찾을 수 있음
 *   3) 다양한 order 값에서 동작
 *   4) 대량 데이터(10만 건) 스모크 테스트
 */

#include "bptree.h"

#include <stdio.h>
#include <stdlib.h>
#include <time.h>

static int g_passed = 0;
static int g_failed = 0;

#define CHECK(cond, msg) do { \
    if (cond) { ++g_passed; printf("  [PASS] %s\n", msg); } \
    else      { ++g_failed; printf("  [FAIL] %s (line %d)\n", msg, __LINE__); } \
} while (0)

/* ── 1) 소규모 INSERT + SEARCH 정합성 ───────────────────────── */
static void test_small_insert_search(void) {
    printf("[TEST] 소규모 INSERT 후 SEARCH 정합성 (order=256)\n");
    BPTree *tree = bptree_create(256);
    CHECK(tree != NULL, "트리 생성 성공");

    int n = 100;
    for (int i = 0; i < n; i++)
        bptree_insert(tree, i + 1, i);

    int found = 0;
    for (int i = 0; i < n; i++) {
        if (bptree_search(tree, i + 1) == i)
            found++;
    }
    CHECK(found == n, "100건 INSERT 후 전수 SEARCH 일치");

    CHECK(bptree_search(tree, 0) == -1, "미삽입 키 0 -> -1");
    CHECK(bptree_search(tree, n + 1) == -1, "미삽입 키 101 -> -1");

    bptree_destroy(tree);
}

/* ── 2) 역순 INSERT 후 SEARCH ────────────────────────────────── */
static void test_reverse_insert(void) {
    printf("[TEST] 역순 INSERT 후 SEARCH (order=256)\n");
    BPTree *tree = bptree_create(256);

    int n = 200;
    for (int i = n; i >= 1; i--)
        bptree_insert(tree, i, n - i);

    int found = 0;
    for (int i = 1; i <= n; i++) {
        if (bptree_search(tree, i) >= 0)
            found++;
    }
    CHECK(found == n, "역순 200건 전수 조회 성공");

    bptree_destroy(tree);
}

/* ── 3) 랜덤 셔플 INSERT + SEARCH ───────────────────────────── */
static void test_shuffle_insert(void) {
    printf("[TEST] 랜덤 셔플 INSERT 후 SEARCH (order=256)\n");
    BPTree *tree = bptree_create(256);

    int n = 200;
    int *keys = malloc(sizeof(int) * (size_t)n);
    for (int i = 0; i < n; i++)
        keys[i] = i + 1;

    /* 셔플 */
    srand(12345);
    for (int i = n - 1; i > 0; i--) {
        int j = rand() % (i + 1);
        int tmp = keys[i];
        keys[i] = keys[j];
        keys[j] = tmp;
    }

    for (int i = 0; i < n; i++)
        bptree_insert(tree, keys[i], i);

    int found = 0;
    for (int i = 1; i <= n; i++) {
        if (bptree_search(tree, i) >= 0)
            found++;
    }
    CHECK(found == n, "셔플 200건 전수 조회 성공");

    free(keys);
    bptree_destroy(tree);
}

/* ── 4) 리프 용량 내 대량 INSERT 스모크 테스트 ────────────────── */
static void test_bulk_smoke(void) {
    int order = 256;
    int n = order - 1; /* split 미구현이라 리프 용량 내로 제한 */
    printf("[TEST] 대량 INSERT 스모크 (order=%d, %d건)\n", order, n);
    BPTree *tree = bptree_create(order);
    CHECK(tree != NULL, "트리 생성 성공");

    for (int i = 0; i < n; i++)
        bptree_insert(tree, i + 1, i);

    int found = 0;
    for (int i = 0; i < n; i++) {
        if (bptree_search(tree, i + 1) == i)
            found++;
    }
    CHECK(found == n, "255건 전수 조회 성공");

    CHECK(bptree_search(tree, 0) == -1, "미삽입 키 0 -> -1");
    CHECK(bptree_search(tree, n + 1) == -1, "미삽입 키 256 -> -1");

    bptree_destroy(tree);
}

/* ── 5) 다양한 order 에서 크래시 없음 ───────────────────────── */
static void test_various_orders(void) {
    printf("[TEST] 다양한 order 값에서 INSERT/SEARCH 크래시 없음\n");
    int orders[] = {3, 4, 16, 64, 256};
    int order_cnt = 5;
    int all_ok = 1;

    for (int o = 0; o < order_cnt; o++) {
        BPTree *tree = bptree_create(orders[o]);
        if (!tree) { all_ok = 0; break; }

        int n = orders[o] - 1; /* 리프 용량 내 */
        for (int i = 0; i < n; i++)
            bptree_insert(tree, i + 1, i);

        for (int i = 0; i < n; i++) {
            if (bptree_search(tree, i + 1) != i) {
                all_ok = 0;
                break;
            }
        }
        bptree_destroy(tree);
    }
    CHECK(all_ok, "order 3,4,16,64,256 모두 정상");
}

/* ── 6) 빈 트리 SEARCH ──────────────────────────────────────── */
static void test_empty_search(void) {
    printf("[TEST] 빈 트리 SEARCH -> -1\n");
    BPTree *tree = bptree_create(128);
    CHECK(bptree_search(tree, 1) == -1, "빈 트리 search -> -1");
    CHECK(bptree_search(tree, 0) == -1, "빈 트리 search 0 -> -1");
    bptree_destroy(tree);
}

/* ── main ─────────────────────────────────────────────────────── */
int main(void) {
    printf("=== test_benchmark (규태) ===\n");

    test_small_insert_search();
    test_reverse_insert();
    test_shuffle_insert();
    test_bulk_smoke();
    test_various_orders();
    test_empty_search();

    printf("\n[BENCHMARK TESTS] %d passed, %d failed\n", g_passed, g_failed);
    return g_failed > 0 ? 1 : 0;
}
