/* bench/tree_shape.c — 라이브 B+ 트리 형상 덤프.
 *
 * 사용법:
 *   ./tree_shape <N> [order] [--snapshots K]
 *
 * 기본: N 개의 id (1..N) 를 셔플 삽입 후 bptree_print 로 최종 트리만 출력.
 * --snapshots K: 삽입 과정에서 K 개의 중간 단계 스냅샷을 생성.
 *   각 스냅샷은 "=== SNAPSHOT step=S inserted=X ===" 헤더 후 bptree_print.
 *   웹 데모가 이걸 replay 해 "삽입 애니메이션" 을 보여준다.
 */

#include "bptree.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void shuffle(int *arr, int n) {
    for (int i = n - 1; i > 0; --i) {
        int j = rand() % (i + 1);
        int t = arr[i]; arr[i] = arr[j]; arr[j] = t;
    }
}

int main(int argc, char **argv) {
    int n = 20;
    int order = 4;
    int snapshots = 0;

    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "--snapshots") == 0 && i + 1 < argc) {
            snapshots = atoi(argv[++i]);
        } else if (argv[i][0] != '-') {
            /* 위치 인자: 첫 번째 N, 두 번째 order */
            static int pos = 0;
            if (pos == 0) n = atoi(argv[i]);
            else if (pos == 1) order = atoi(argv[i]);
            pos++;
        }
    }

    if (n <= 0) n = 20;
    if (order < 3) order = 4;
    if (n > 200) n = 200;
    if (snapshots < 0) snapshots = 0;
    if (snapshots > n) snapshots = n;

    srand(42);
    int *keys = malloc(sizeof(int) * (size_t)n);
    if (!keys) return 1;
    for (int i = 0; i < n; ++i) keys[i] = i + 1;
    shuffle(keys, n);

    BPTree *t = bptree_create(order);
    if (!t) { free(keys); return 1; }

    printf("== B+ Tree shape (N=%d, order=%d, insert order shuffled%s) ==\n",
           n, order, snapshots > 0 ? ", snapshots" : "");

    /* snapshots 모드: 균등 분포 지점에서 트리 덤프 */
    int snap_interval = snapshots > 0 ? (n + snapshots - 1) / snapshots : 0;
    int next_snap = snap_interval;

    for (int i = 0; i < n; ++i) {
        bptree_insert(t, keys[i], i);
        int inserted = i + 1;
        if (snapshots > 0 && (inserted == next_snap || inserted == n)) {
            printf("=== SNAPSHOT step=%d inserted=%d ===\n",
                   (int)((long)inserted * snapshots / n), inserted);
            bptree_print(t);
            next_snap += snap_interval;
        }
    }
    if (snapshots == 0) {
        bptree_print(t);
    }

    bptree_destroy(t);
    free(keys);
    return 0;
}
