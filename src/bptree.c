/* bptree.c — MiniSQL Week 7 B+ Tree 인덱스 구현 (지용 담당).
 *
 * 설계:
 *   - key = int (id 컬럼), value = int row_index (storage CSV 행 번호).
 *   - 모든 값(row_index) 은 리프에만 저장 (B+ 트리 기본 성질).
 *   - 리프끼리 next 포인터로 연결 → range 쿼리 O(log n + k) 에 유리.
 *   - 한 노드의 최대 자식 수 = order, 최대 키 수 = order - 1.
 *   - 배열은 order(+1) 크기로 잡아 overflow 순간에도 한 슬롯 여유.
 *
 * 노드 표현:
 *   Node 하나로 리프/내부 양쪽 다 표현. is_leaf 플래그로 구분하고
 *   leaf / internal 전용 데이터는 union 으로 둔다.
 *
 * Phase 2: 구조체 + create/destroy + search.
 * Phase 3: insert (split 없음).
 * Phase 4: leaf split + root grow.
 * Phase 5: internal split + root split (이번 커밋). 트리 성장 완성.
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

/* 새 리프 노드 할당. 배열은 order 크기 — num_keys 가 (order-1) 을 넘어
 * 잠시 order 가 되는 순간 split 을 트리거한다. */
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

/* 새 내부 노드 할당.
 * keys 는 order 크기 (최대 order-1 + overflow 순간 1), children 는 order+1. */
static Node *internal_new(int order) {
    Node *n = malloc(sizeof *n);
    if (!n) return NULL;
    n->is_leaf = 0;
    n->num_keys = 0;
    n->keys = malloc(sizeof(int) * (size_t)order);
    n->u.internal.children = malloc(sizeof(Node *) * (size_t)(order + 1));
    if (!n->keys || !n->u.internal.children) {
        free(n->keys);
        free(n->u.internal.children);
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

/* 내부 노드에서 id 키가 속할 자식 인덱스 반환.
 *
 * 규약: keys[i-1] <= id < keys[i] 인 i 를 찾아 children[i] 로 내려간다. */
static int internal_child_idx(const Node *n, int id) {
    assert(!n->is_leaf);
    int i = 0;
    while (i < n->num_keys && id >= n->keys[i]) {
        ++i;
    }
    return i;
}

/* 리프에 정렬 상태 유지하며 삽입. 중복 키면 row_index 덮어쓰기.
 * 배열은 order 크기라 num_keys 가 order-1 일 때 한 번 더 넣어 order 로 올리는
 * overflow 를 허용 (직후 split 호출 전제). */
static void leaf_insert_sorted(Node *leaf, int id, int row_index) {
    assert(leaf->is_leaf);
    int i = 0;
    while (i < leaf->num_keys && leaf->keys[i] < id) ++i;
    if (i < leaf->num_keys && leaf->keys[i] == id) {
        leaf->u.leaf.row_indices[i] = row_index;
        return;
    }
    for (int j = leaf->num_keys; j > i; --j) {
        leaf->keys[j] = leaf->keys[j - 1];
        leaf->u.leaf.row_indices[j] = leaf->u.leaf.row_indices[j - 1];
    }
    leaf->keys[i] = id;
    leaf->u.leaf.row_indices[i] = row_index;
    ++leaf->num_keys;
}

/* leaf 를 왼/오른쪽 리프로 분할.
 * 왼쪽 (원본 leaf) 은 앞쪽 절반을 유지, 오른쪽(반환값)은 뒷쪽 절반을 가져간다.
 * linked list (next 포인터) 에 새 리프 삽입.
 * 부모로 승격될 키는 오른쪽 리프의 첫 키 (= 오른쪽이 책임지는 최소값).
 *
 * 반환: 새로 만든 오른쪽 리프 (NULL 이면 할당 실패). */
static Node *split_leaf(Node *leaf, int order, int *promoted_key) {
    assert(leaf->is_leaf);
    assert(leaf->num_keys == order); /* overflow 직후 호출 */
    Node *right = leaf_new(order);
    if (!right) return NULL;
    int mid = order / 2; /* 왼쪽에 mid 개, 오른쪽에 order-mid 개. */
    for (int i = mid; i < order; ++i) {
        right->keys[i - mid] = leaf->keys[i];
        right->u.leaf.row_indices[i - mid] = leaf->u.leaf.row_indices[i];
    }
    right->num_keys = order - mid;
    leaf->num_keys = mid;
    right->u.leaf.next = leaf->u.leaf.next;
    leaf->u.leaf.next = right;
    *promoted_key = right->keys[0];
    return right;
}

/* 내부 노드에 (key, right_child) 삽입. 이미 공간 있는 상태만 호출.
 * key 는 정렬 위치에, right_child 는 해당 key 의 오른쪽 자식 슬롯에 들어간다. */
static void internal_insert_nonfull(Node *n, int key, Node *right_child) {
    assert(!n->is_leaf);
    int i = 0;
    while (i < n->num_keys && key >= n->keys[i]) ++i;
    for (int j = n->num_keys; j > i; --j) n->keys[j] = n->keys[j - 1];
    for (int j = n->num_keys + 1; j > i + 1; --j) {
        n->u.internal.children[j] = n->u.internal.children[j - 1];
    }
    n->keys[i] = key;
    n->u.internal.children[i + 1] = right_child;
    ++n->num_keys;
}

/* 내부 노드를 왼/오른으로 분할.
 * 리프와 달리 중간 키는 양쪽 어디에도 남지 않고 완전히 위로 승격된다.
 * 왼쪽(원본): keys[0..mid-1], children[0..mid]
 * 승격:      keys[mid]
 * 오른쪽:    keys[mid+1..order-1], children[mid+1..order]
 *
 * 호출 전제: n->num_keys == order (overflow 상태). */
static Node *split_internal(Node *n, int order, int *promoted_key) {
    assert(!n->is_leaf);
    assert(n->num_keys == order);
    Node *right = internal_new(order);
    if (!right) return NULL;
    int mid = order / 2;
    *promoted_key = n->keys[mid];
    int right_keys = order - mid - 1;
    for (int i = 0; i < right_keys; ++i) {
        right->keys[i] = n->keys[mid + 1 + i];
    }
    for (int i = 0; i <= right_keys; ++i) {
        right->u.internal.children[i] = n->u.internal.children[mid + 1 + i];
    }
    right->num_keys = right_keys;
    n->num_keys = mid;
    return right;
}

/* 재귀 삽입. split 이 발생하면 split_out / key_out 으로 상위에 전달.
 *
 * 반환: 0 = split 없음, 1 = split 발생 (상위에서 key_out, split_out 처리 필요). */
static int insert_rec(Node *n, int order, int id, int row_index,
                      Node **split_out, int *key_out) {
    if (n->is_leaf) {
        leaf_insert_sorted(n, id, row_index);
        if (n->num_keys < order) return 0; /* 여유 있으면 그대로. */
        Node *right = split_leaf(n, order, key_out);
        if (!right) return 0; /* 할당 실패 — 데이터 드롭. 실전에선 에러 반환 필요. */
        *split_out = right;
        return 1;
    }

    int ci = internal_child_idx(n, id);
    Node *child_split = NULL;
    int child_key = 0;
    int split = insert_rec(n->u.internal.children[ci], order, id, row_index,
                           &child_split, &child_key);
    if (!split) return 0;

    /* 자식에서 split 이 올라왔다. 일단 이 내부 노드에 (child_key, child_split)
     * 를 정렬 위치에 삽입 (배열 여유로 overflow 허용). 그 다음 자체 split 판단. */
    internal_insert_nonfull(n, child_key, child_split);
    if (n->num_keys < order) return 0;

    Node *right = split_internal(n, order, key_out);
    if (!right) return 0;
    *split_out = right;
    return 1;
}

void bptree_insert(BPTree *tree, int id, int row_index) {
    if (!tree || !tree->root) return;
    Node *sibling = NULL;
    int promoted_key = 0;
    int split = insert_rec(tree->root, tree->order, id, row_index, &sibling, &promoted_key);
    if (!split) return;

    /* 루트 자체가 분할됐다. 새 내부 노드를 만들어 트리 높이 +1. */
    Node *new_root = internal_new(tree->order);
    if (!new_root) return; /* 할당 실패 — 트리는 비정상이지만 누수 방지 최소 처리. */
    new_root->keys[0] = promoted_key;
    new_root->u.internal.children[0] = tree->root;
    new_root->u.internal.children[1] = sibling;
    new_root->num_keys = 1;
    tree->root = new_root;
}

int bptree_search(BPTree *tree, int id) {
    if (!tree || !tree->root) return -1;
    Node *cur = tree->root;
    while (!cur->is_leaf) {
        cur = cur->u.internal.children[internal_child_idx(cur, id)];
    }
    for (int i = 0; i < cur->num_keys; ++i) {
        if (cur->keys[i] == id) {
            return cur->u.leaf.row_indices[i];
        }
    }
    return -1;
}

/* [from, to] 범위의 row_index 들을 leaf linked list 로 순회하며 out 에 채움.
 * 양끝 포함. from > to 이거나 인자가 비정상이면 0 반환. */
int bptree_range(BPTree *tree, int from, int to, int *out, int max_out) {
    if (!tree || !tree->root || !out || max_out <= 0 || from > to) return 0;

    /* from 이 속할 리프까지 descent. */
    Node *cur = tree->root;
    while (!cur->is_leaf) {
        cur = cur->u.internal.children[internal_child_idx(cur, from)];
    }

    /* 리프 linked list 를 따라가며 [from, to] 범위 수집. */
    int filled = 0;
    while (cur && filled < max_out) {
        for (int i = 0; i < cur->num_keys && filled < max_out; ++i) {
            int k = cur->keys[i];
            if (k < from) continue;
            if (k > to) return filled; /* 정렬돼 있으므로 더 볼 필요 없음. */
            out[filled++] = cur->u.leaf.row_indices[i];
        }
        cur = cur->u.leaf.next;
    }
    return filled;
}

/* 노드를 depth 만큼 들여쓰기로 출력. 내부 노드는 keys 를, 리프 노드는
 * (key→row_index) 쌍과 next 링크 유무를 보여준다. */
static void print_node(const Node *n, int depth) {
    if (!n) return;
    for (int d = 0; d < depth; ++d) fputs("  ", stdout);
    if (n->is_leaf) {
        printf("LEAF[");
        for (int i = 0; i < n->num_keys; ++i) {
            if (i) fputs(", ", stdout);
            printf("%d->%d", n->keys[i], n->u.leaf.row_indices[i]);
        }
        printf("]%s\n", n->u.leaf.next ? " ->" : "");
    } else {
        printf("INT[");
        for (int i = 0; i < n->num_keys; ++i) {
            if (i) fputs(", ", stdout);
            printf("%d", n->keys[i]);
        }
        printf("]\n");
        for (int i = 0; i <= n->num_keys; ++i) {
            print_node(n->u.internal.children[i], depth + 1);
        }
    }
}

void bptree_print(BPTree *tree) {
    if (!tree || !tree->root) {
        printf("(empty tree)\n");
        return;
    }
    printf("BPTree(order=%d):\n", tree->order);
    print_node(tree->root, 1);
}
