/* tests/test_index_registry.c — index_registry 단위 테스트. */

#include "index_registry.h"
#include "bptree.h"

#include <stdio.h>
#include <string.h>

static int g_passed = 0;
static int g_failed = 0;

#define CHECK(cond, msg) do { \
    if (cond) { ++g_passed; printf("  [PASS] %s\n", msg); } \
    else      { ++g_failed; printf("  [FAIL] %s (line %d)\n", msg, __LINE__); } \
} while (0)

static void test_empty_get_returns_null(void) {
    printf("[TEST] 빈 레지스트리에서 get → NULL\n");
    index_registry_destroy_all();
    CHECK(index_registry_get("users") == NULL, "미등록 테이블은 NULL");
    CHECK(index_registry_get(NULL) == NULL, "NULL 이름도 NULL");
}

static void test_get_or_create_creates(void) {
    printf("[TEST] get_or_create 는 없으면 만들고 있으면 재사용\n");
    index_registry_destroy_all();
    BPTree *t1 = index_registry_get_or_create("users", 128);
    CHECK(t1 != NULL, "첫 호출 시 새 트리 생성");
    BPTree *t2 = index_registry_get_or_create("users", 128);
    CHECK(t1 == t2, "동일 테이블 두 번째 호출은 같은 포인터");
    BPTree *got = index_registry_get("users");
    CHECK(got == t1, "get 도 같은 포인터 반환");
    index_registry_destroy_all();
}

static void test_multiple_tables_isolated(void) {
    printf("[TEST] 여러 테이블은 각각 다른 트리\n");
    index_registry_destroy_all();
    BPTree *a = index_registry_get_or_create("users", 128);
    BPTree *b = index_registry_get_or_create("orders", 128);
    BPTree *c = index_registry_get_or_create("products", 128);
    CHECK(a && b && c, "세 테이블 모두 생성 성공");
    CHECK(a != b && b != c && a != c, "서로 다른 인스턴스");

    bptree_insert(a, 1, 100);
    bptree_insert(b, 1, 200);
    CHECK(bptree_search(a, 1) == 100, "users 테이블 id=1 → 100");
    CHECK(bptree_search(b, 1) == 200, "orders 테이블 id=1 → 200 (분리)");
    index_registry_destroy_all();
}

static void test_destroy_all_is_idempotent(void) {
    printf("[TEST] destroy_all 은 두 번 호출해도 안전\n");
    index_registry_get_or_create("users", 128);
    index_registry_destroy_all();
    index_registry_destroy_all();
    CHECK(index_registry_get("users") == NULL, "destroy 후 get → NULL");
}

static void test_invalid_order_fails(void) {
    printf("[TEST] 잘못된 order 는 NULL 반환\n");
    index_registry_destroy_all();
    CHECK(index_registry_get_or_create("t", 2) == NULL, "order=2 실패");
    CHECK(index_registry_get("t") == NULL, "실패 시 등록도 안 됨");
}

static void test_reuse_after_destroy(void) {
    printf("[TEST] destroy 후 재등록 가능\n");
    index_registry_destroy_all();
    BPTree *a = index_registry_get_or_create("users", 128);
    bptree_insert(a, 5, 50);
    CHECK(bptree_search(a, 5) == 50, "등록 후 조회");
    index_registry_destroy_all();
    BPTree *b = index_registry_get_or_create("users", 128);
    CHECK(b != NULL, "재등록 성공");
    CHECK(bptree_search(b, 5) == -1, "새 트리는 이전 데이터 없음");
    index_registry_destroy_all();
}

int main(void) {
    printf("=== test_index_registry ===\n");
    test_empty_get_returns_null();
    test_get_or_create_creates();
    test_multiple_tables_isolated();
    test_destroy_all_is_idempotent();
    test_invalid_order_fails();
    test_reuse_after_destroy();
    printf("\n[REGISTRY TESTS] %d passed, %d failed\n", g_passed, g_failed);
    return g_failed == 0 ? 0 : 1;
}
