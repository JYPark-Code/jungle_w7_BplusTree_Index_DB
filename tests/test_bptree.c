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

/* ---------- Phase 3: insert (split 없는 버전) ---------- */

static void test_insert_single(void) {
    printf("[TEST] 단일 삽입 후 search 성공\n");
    BPTree *t = bptree_create(8);
    bptree_insert(t, 42, 100);
    CHECK(bptree_search(t, 42) == 100, "id=42 -> row_index=100");
    CHECK(bptree_search(t, 41) == -1,  "id=41 은 없음");
    CHECK(bptree_search(t, 43) == -1,  "id=43 은 없음");
    bptree_destroy(t);
}

static void test_insert_sorted_order(void) {
    printf("[TEST] 오름차순 삽입 후 전수 search\n");
    BPTree *t = bptree_create(8); /* order-1 = 7 개 수용 */
    for (int i = 0; i < 7; ++i) bptree_insert(t, i * 10, i);
    int ok = 1;
    for (int i = 0; i < 7; ++i) {
        if (bptree_search(t, i * 10) != i) ok = 0;
    }
    CHECK(ok, "0,10,20,...,60 전수 조회 성공");
    bptree_destroy(t);
}

static void test_insert_reverse_order(void) {
    printf("[TEST] 내림차순 삽입 후 정렬 유지 검증\n");
    BPTree *t = bptree_create(8);
    for (int i = 6; i >= 0; --i) bptree_insert(t, i * 10, i);
    int ok = 1;
    for (int i = 0; i < 7; ++i) {
        if (bptree_search(t, i * 10) != i) ok = 0;
    }
    CHECK(ok, "역순 삽입 후에도 모든 키 조회 성공");
    bptree_destroy(t);
}

static void test_insert_interleaved(void) {
    printf("[TEST] 뒤섞인 순서 삽입 + search\n");
    BPTree *t = bptree_create(8);
    int keys[] = {30, 10, 50, 20, 60, 40, 0};
    int rows[] = {3, 1, 5, 2, 6, 4, 0};
    for (int i = 0; i < 7; ++i) bptree_insert(t, keys[i], rows[i]);
    int ok = 1;
    for (int i = 0; i < 7; ++i) {
        if (bptree_search(t, keys[i]) != rows[i]) ok = 0;
    }
    CHECK(ok, "뒤섞인 삽입 7건 전수 조회 성공");
    bptree_destroy(t);
}

static void test_insert_duplicate_overwrites(void) {
    printf("[TEST] 중복 키 삽입은 row_index 덮어쓰기\n");
    BPTree *t = bptree_create(8);
    bptree_insert(t, 5, 100);
    bptree_insert(t, 5, 999);
    CHECK(bptree_search(t, 5) == 999, "동일 id 재삽입 시 최신 row 로 갱신");
    bptree_destroy(t);
}

static void test_insert_capacity_limit(void) {
    printf("[TEST] Phase 4: 리프 용량 초과 → leaf split 으로 전부 유지\n");
    BPTree *t = bptree_create(4); /* 리프 당 max 3 키, 4번째가 split 트리거 */
    bptree_insert(t, 1, 10);
    bptree_insert(t, 2, 20);
    bptree_insert(t, 3, 30);
    bptree_insert(t, 4, 40);
    CHECK(bptree_search(t, 1) == 10, "split 후 id=1 유지");
    CHECK(bptree_search(t, 2) == 20, "split 후 id=2 유지");
    CHECK(bptree_search(t, 3) == 30, "split 후 id=3 유지");
    CHECK(bptree_search(t, 4) == 40, "split 로 승격된 새 리프의 id=4 조회 성공");
    bptree_destroy(t);
}

/* ---------- Phase 4: leaf split + root grow ---------- */

static void test_leaf_split_ascending(void) {
    printf("[TEST] order=4 오름차순 삽입 9건 (leaf split 다수, 내부 full 직전)\n");
    BPTree *t = bptree_create(4);
    int n = 9; /* Phase 4: 내부 노드 split 전까지만 — Phase 5 에서 확장 */
    for (int i = 1; i <= n; ++i) bptree_insert(t, i, i * 100);
    int ok = 1;
    for (int i = 1; i <= n; ++i) {
        if (bptree_search(t, i) != i * 100) ok = 0;
    }
    CHECK(ok, "1..9 오름차순 삽입 후 전수 조회 성공");
    bptree_destroy(t);
}

static void test_leaf_split_descending(void) {
    printf("[TEST] order=4 내림차순 삽입 9건\n");
    BPTree *t = bptree_create(4);
    int n = 9;
    for (int i = n; i >= 1; --i) bptree_insert(t, i, i * 7);
    int ok = 1;
    for (int i = 1; i <= n; ++i) {
        if (bptree_search(t, i) != i * 7) ok = 0;
    }
    CHECK(ok, "내림차순 삽입 후 전수 조회 성공");
    bptree_destroy(t);
}

static void test_leaf_split_mixed_larger(void) {
    printf("[TEST] order=16 로 15건 뒤섞어 삽입 (leaf 여러 번 split, 내부 안정)\n");
    BPTree *t = bptree_create(16);
    int keys[15];
    for (int i = 0; i < 15; ++i) keys[i] = (i * 7 + 3) % 15; /* 순열 */
    for (int i = 0; i < 15; ++i) bptree_insert(t, keys[i], keys[i] * 10);
    int ok = 1;
    for (int i = 0; i < 15; ++i) {
        if (bptree_search(t, keys[i]) != keys[i] * 10) ok = 0;
    }
    CHECK(ok, "뒤섞인 15건 전수 조회 성공");
    bptree_destroy(t);
}

static void test_leaf_split_root_grows(void) {
    printf("[TEST] 리프만 있던 트리가 split 후 내부 루트로 성장\n");
    BPTree *t = bptree_create(4);
    /* order=4 → 리프 max 3 키. 4건 삽입하면 루트가 내부 노드로 승격 */
    bptree_insert(t, 10, 1);
    bptree_insert(t, 20, 2);
    bptree_insert(t, 30, 3);
    CHECK(bptree_search(t, 10) == 1 && bptree_search(t, 20) == 2 && bptree_search(t, 30) == 3,
          "split 직전 상태: 단일 리프");
    bptree_insert(t, 40, 4);
    CHECK(bptree_search(t, 10) == 1, "root grow 후 id=10 조회");
    CHECK(bptree_search(t, 40) == 4, "root grow 후 id=40 조회");
    CHECK(bptree_search(t, 25) == -1, "없는 키는 -1");
    bptree_destroy(t);
}

static void test_leaf_split_duplicate_after_split(void) {
    printf("[TEST] split 된 트리에서도 중복 삽입은 덮어쓰기\n");
    BPTree *t = bptree_create(4);
    for (int i = 1; i <= 6; ++i) bptree_insert(t, i, i * 10);
    bptree_insert(t, 3, 999);
    bptree_insert(t, 6, 777);
    CHECK(bptree_search(t, 3) == 999, "왼쪽 리프의 중복 키 덮어쓰기");
    CHECK(bptree_search(t, 6) == 777, "오른쪽 리프의 중복 키 덮어쓰기");
    CHECK(bptree_search(t, 1) == 10, "무관한 키는 유지");
    bptree_destroy(t);
}

/* ---------- Phase 5: internal split + 다단 트리 ---------- */

static void test_internal_split_small_order(void) {
    printf("[TEST] order=4 로 50건 삽입 (내부 split 여러 번, 트리 3+ 레벨)\n");
    BPTree *t = bptree_create(4);
    int n = 50;
    for (int i = 1; i <= n; ++i) bptree_insert(t, i, i * 3);
    int ok = 1;
    for (int i = 1; i <= n; ++i) {
        if (bptree_search(t, i) != i * 3) ok = 0;
    }
    CHECK(ok, "1..50 전수 조회 성공 (내부 split 다수 유발)");
    bptree_destroy(t);
}

static void test_internal_split_descending_large(void) {
    printf("[TEST] order=4 내림차순 80건 삽입\n");
    BPTree *t = bptree_create(4);
    int n = 80;
    for (int i = n; i >= 1; --i) bptree_insert(t, i, -i);
    int ok = 1;
    for (int i = 1; i <= n; ++i) {
        if (bptree_search(t, i) != -i) ok = 0;
    }
    CHECK(ok, "내림차순 80건 전수 조회 성공");
    bptree_destroy(t);
}

static void test_internal_split_shuffled(void) {
    printf("[TEST] order=4 뒤섞인 100건 (순열) 삽입\n");
    BPTree *t = bptree_create(4);
    int n = 100;
    int keys[100];
    /* 간단한 순열: i*37 mod 100 은 gcd(37,100)=1 이라 bijection. */
    for (int i = 0; i < n; ++i) keys[i] = (i * 37) % n;
    for (int i = 0; i < n; ++i) bptree_insert(t, keys[i], keys[i] + 1000);
    int ok = 1;
    for (int i = 0; i < n; ++i) {
        if (bptree_search(t, i) != i + 1000) ok = 0;
    }
    CHECK(ok, "순열 100건 전수 조회 성공");
    bptree_destroy(t);
}

static void test_internal_split_large_scale(void) {
    printf("[TEST] order=8, 1000 건 삽입 후 전수 검증\n");
    BPTree *t = bptree_create(8);
    int n = 1000;
    for (int i = 0; i < n; ++i) bptree_insert(t, i * 2, i); /* 짝수만 */
    int ok = 1;
    for (int i = 0; i < n; ++i) {
        if (bptree_search(t, i * 2) != i) ok = 0;
    }
    CHECK(ok, "짝수 1000건 전수 조회");
    /* 홀수는 없어야 함 */
    int not_found_ok = 1;
    for (int i = 0; i < 20; ++i) {
        if (bptree_search(t, i * 2 + 1) != -1) not_found_ok = 0;
    }
    CHECK(not_found_ok, "삽입 안 한 홀수 키 20개는 -1");
    bptree_destroy(t);
}

static void test_internal_split_various_orders(void) {
    printf("[TEST] order 3,5,16 에서도 200건 삽입이 정상 동작\n");
    int orders[] = {3, 5, 16};
    int all_ok = 1;
    for (int oi = 0; oi < 3; ++oi) {
        BPTree *t = bptree_create(orders[oi]);
        for (int i = 0; i < 200; ++i) bptree_insert(t, i, i);
        for (int i = 0; i < 200; ++i) {
            if (bptree_search(t, i) != i) { all_ok = 0; break; }
        }
        bptree_destroy(t);
    }
    CHECK(all_ok, "order 3/5/16 모두 200건 전수 조회 통과");
}

static void test_insert_null_tree(void) {
    printf("[TEST] NULL 트리 삽입은 no-op\n");
    bptree_insert(NULL, 1, 1);
    CHECK(1, "crash 없이 통과");
}

int main(void) {
    printf("=== test_bptree (Phase 5) ===\n");
    test_create_with_valid_order();
    test_create_rejects_tiny_order();
    test_create_accepts_large_order();
    test_destroy_null_safe();
    test_search_on_empty_tree();
    test_search_null_tree();
    test_range_stub_returns_zero();
    test_multiple_create_destroy();
    test_insert_single();
    test_insert_sorted_order();
    test_insert_reverse_order();
    test_insert_interleaved();
    test_insert_duplicate_overwrites();
    test_insert_capacity_limit();
    test_insert_null_tree();
    test_leaf_split_ascending();
    test_leaf_split_descending();
    test_leaf_split_mixed_larger();
    test_leaf_split_root_grows();
    test_leaf_split_duplicate_after_split();
    test_internal_split_small_order();
    test_internal_split_descending_large();
    test_internal_split_shuffled();
    test_internal_split_large_scale();
    test_internal_split_various_orders();

    printf("\n[BPTREE TESTS] %d passed, %d failed\n", g_passed, g_failed);
    return g_failed == 0 ? 0 : 1;
}
