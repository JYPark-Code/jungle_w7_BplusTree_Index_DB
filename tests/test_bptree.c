/* tests/test_bptree.c — Phase 2 (struct + create/destroy + search) 단위 테스트.
 *
 * 아직 bptree_insert 가 스텁이므로, 이 단계에서 검증 가능한 것은:
 *   1) bptree_create 가 유효한 트리 + 빈 리프 루트를 만든다.
 *   2) bptree_destroy 가 NULL 안전 + 정상 트리 해제 (valgrind 로 확인).
 *   3) bptree_search 는 빈 트리에서 항상 -1.
 *   4) 잘못된 order 는 NULL 반환.
 *
 * Phase 3 (insert) 가 들어오면 대량 삽입 후 전수 검색 케이스를 이어서 추가.
 */

#include "bptree.h"

#include <stdio.h>
#include <stdlib.h>

static int g_passed = 0;
static int g_failed = 0;

#define CHECK(cond, msg) do { \
    if (cond) { ++g_passed; printf("  [PASS] %s\n", msg); } \
    else      { ++g_failed; printf("  [FAIL] %s (line %d)\n", msg, __LINE__); } \
} while (0)

static void test_create_with_valid_order(void) {
    printf("[TEST] create with valid order (order=4)\n");
    BPTree *t = bptree_create(4);
    CHECK(t != NULL, "bptree_create(4) returns non-NULL");
    bptree_destroy(t);
}

static void test_create_rejects_tiny_order(void) {
    printf("[TEST] create rejects order < 3\n");
    CHECK(bptree_create(0) == NULL, "order=0 returns NULL");
    CHECK(bptree_create(1) == NULL, "order=1 returns NULL");
    CHECK(bptree_create(2) == NULL, "order=2 returns NULL");
    CHECK(bptree_create(-5) == NULL, "negative order returns NULL");
}

static void test_create_accepts_large_order(void) {
    printf("[TEST] create accepts a large order (order=256)\n");
    BPTree *t = bptree_create(256);
    CHECK(t != NULL, "bptree_create(256) returns non-NULL");
    bptree_destroy(t);
}

static void test_destroy_null_safe(void) {
    printf("[TEST] destroy is NULL-safe\n");
    bptree_destroy(NULL);
    CHECK(1, "bptree_destroy(NULL) did not crash");
}

static void test_search_on_empty_tree(void) {
    printf("[TEST] search on empty tree returns -1\n");
    BPTree *t = bptree_create(4);
    CHECK(bptree_search(t, 0) == -1,   "search id=0 on empty -> -1");
    CHECK(bptree_search(t, 42) == -1,  "search id=42 on empty -> -1");
    CHECK(bptree_search(t, -7) == -1,  "search negative id on empty -> -1");
    bptree_destroy(t);
}

static void test_search_null_tree(void) {
    printf("[TEST] search on NULL tree returns -1\n");
    CHECK(bptree_search(NULL, 5) == -1, "bptree_search(NULL, ...) -> -1");
}

static void test_range_stub_returns_zero(void) {
    printf("[TEST] range stub returns 0 (Phase 2 미구현)\n");
    BPTree *t = bptree_create(4);
    int out[8];
    CHECK(bptree_range(t, 0, 100, out, 8) == 0, "range on empty tree -> 0");
    bptree_destroy(t);
}

static void test_multiple_create_destroy(void) {
    printf("[TEST] 여러 트리 동시 생성/해제 (누수 검증용)\n");
    BPTree *a = bptree_create(4);
    BPTree *b = bptree_create(8);
    BPTree *c = bptree_create(16);
    CHECK(a && b && c, "세 트리 모두 생성 성공");
    bptree_destroy(b);
    bptree_destroy(a);
    bptree_destroy(c);
    CHECK(1, "역순 해제 이슈 없음");
}

int main(void) {
    printf("=== test_bptree (Phase 2) ===\n");
    test_create_with_valid_order();
    test_create_rejects_tiny_order();
    test_create_accepts_large_order();
    test_destroy_null_safe();
    test_search_on_empty_tree();
    test_search_null_tree();
    test_range_stub_returns_zero();
    test_multiple_create_destroy();

    printf("\n[BPTREE TESTS] %d passed, %d failed\n", g_passed, g_failed);
    return g_failed == 0 ? 0 : 1;
}
