/* bench/tree_shape.c — 라이브 B+ 트리 형상 덤프.
 *
 * 사용법:
 *   ./tree_shape <N> [order]
 *
 * N 개의 id (1..N) 를 셔플 삽입 후 bptree_print 로 트리 ASCII 를 stdout 에 출력.
 * 웹 데모 C 섹션 (/api/tree_shape) 에서 subprocess 로 호출해 트리 모양을 시각화.
 */

#include "bptree.h"

#include <stdio.h>
#include <stdlib.h>
#include <time.h>

static void shuffle(int *arr, int n) {
    for (int i = n - 1; i > 0; --i) {
        int j = rand() % (i + 1);
        int t = arr[i]; arr[i] = arr[j]; arr[j] = t;
    }
}

int main(int argc, char **argv) {
    int n = (argc > 1) ? atoi(argv[1]) : 20;
    int order = (argc > 2) ? atoi(argv[2]) : 4;
    if (n <= 0) n = 20;
    if (order < 3) order = 4;
    if (n > 200) n = 200;  /* 출력 폭 방어 */

    srand(42);
    int *keys = malloc(sizeof(int) * (size_t)n);
    if (!keys) return 1;
    for (int i = 0; i < n; ++i) keys[i] = i + 1;
    shuffle(keys, n);

    BPTree *t = bptree_create(order);
    if (!t) { free(keys); return 1; }

    printf("== B+ Tree shape (N=%d, order=%d, insert order shuffled) ==\n", n, order);
    for (int i = 0; i < n; ++i) {
        bptree_insert(t, keys[i], i);
    }
    bptree_print(t);

    bptree_destroy(t);
    free(keys);
    return 0;
}
