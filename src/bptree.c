/* bptree.c — MiniSQL Week 7 B+ Tree 인덱스 구현 (지용 담당).
 *
 * 설계:
 *   - key = int (id 컬럼), value = int row_index (storage CSV 행 번호).
 *   - 모든 값(row_index) 은 리프에만 저장 (B+ 트리 기본 성질).
 *   - 리프끼리 next 포인터로 연결 → range 쿼리 O(log n + k) 에 유리.
 *   - 한 노드의 최대 자식 수 = order, 최대 키 수 = order - 1.
 *
 * 노드 표현:
 *   Node 하나로 리프/내부 양쪽 다 표현. is_leaf 플래그로 구분하고
 *   leaf / internal 전용 데이터는 union 으로 둔다.
 *
 * Phase 2: 구조체 + create/destroy + search.
 * Phase 3 이후: insert + split.
 */

#include "bptree.h"

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

/* 노드 공용 구조.
 * keys[]        : 크기 order (내부노드는 최대 order-1 개 사용, 삽입 중 오버플로 확인용으로 +1 여유).
 * leaf.row_indices[] : 크기 order.
 * internal.children[] : 크기 order+1. */
typedef struct Node {
    int is_leaf;
    int num_keys;
    int *keys;
    union {
        struct {
            int *row_indices;
            struct Node *next;
        } leaf;
        struct {
            struct Node **children;
        } internal;
    } u;
} Node;

struct BPTree {
    int order;
    Node *root;
};

/* 새 리프 노드 할당. 모든 배열은 order 크기로 잡아 overflow 처리에 여유를 둔다. */
static Node *leaf_new(int order) {
    Node *n = malloc(sizeof *n);
    if (!n) return NULL;
    n->is_leaf = 1;
    n->num_keys = 0;
    n->keys = malloc(sizeof(int) * (size_t)order);
    n->u.leaf.row_indices = malloc(sizeof(int) * (size_t)order);
    n->u.leaf.next = NULL;
    if (!n->keys || !n->u.leaf.row_indices) {
        free(n->keys);
        free(n->u.leaf.row_indices);
        free(n);
        return NULL;
    }
    return n;
}

/* 노드 + 하위 트리 재귀 해제. 리프의 next 링크는 "같은 레벨 이웃" 이므로
 * 따라가지 않는다 (부모에서 children 배열로 모두 도달 가능하기 때문). */
static void node_free(Node *n) {
    if (!n) return;
    if (!n->is_leaf) {
        for (int i = 0; i <= n->num_keys; ++i) {
            node_free(n->u.internal.children[i]);
        }
        free(n->u.internal.children);
    } else {
        free(n->u.leaf.row_indices);
    }
    free(n->keys);
    free(n);
}

BPTree *bptree_create(int order) {
    if (order < 3) return NULL; /* 최소 차수 제한 — split 로직이 성립하려면 3 이상. */
    BPTree *t = malloc(sizeof *t);
    if (!t) return NULL;
    t->order = order;
    t->root = leaf_new(order);
    if (!t->root) {
        free(t);
        return NULL;
    }
    return t;
}

void bptree_destroy(BPTree *tree) {
    if (!tree) return;
    node_free(tree->root);
    free(tree);
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
    return -1; /* Phase 2 하위 커밋에서 구현. */
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
