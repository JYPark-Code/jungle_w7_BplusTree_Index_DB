/* bptree.c — MiniSQL Week 7 B+ Tree 인덱스 구현 (지용 담당).
 *
 * MP1 단계에서는 컴파일 가능한 최소 스텁만 제공.
 * Phase 2 부터 구조체 → search → insert → split 순으로 채움.
 *
 * 이 파일 외 다른 파일은 수정 금지 (executor/storage 는 각각 정환/민철).
 */

#include "bptree.h"

#include <stdlib.h>

/* MP1 스텁: 아직 내부 레이아웃 없음. bptree_create 에서 최소 객체만 할당. */
struct BPTree {
    int order;
};

BPTree *bptree_create(int order) {
    BPTree *t = malloc(sizeof *t);
    if (!t) return NULL;
    t->order = order;
    return t;
}

void bptree_insert(BPTree *tree, int id, int row_index) {
    (void)tree;
    (void)id;
    (void)row_index;
    /* Phase 3 에서 구현. */
}

int bptree_search(BPTree *tree, int id) {
    (void)tree;
    (void)id;
    return -1; /* Phase 2 에서 구현. */
}

int bptree_range(BPTree *tree, int from, int to, int *out, int max_out) {
    (void)tree;
    (void)from;
    (void)to;
    (void)out;
    (void)max_out;
    return 0; /* 선택 구현. */
}

void bptree_print(BPTree *tree) {
    (void)tree;
    /* 디버그용, 선택 구현. */
}

void bptree_destroy(BPTree *tree) {
    if (!tree) return;
    free(tree);
}
