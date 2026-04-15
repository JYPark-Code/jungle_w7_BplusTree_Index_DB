/* bptree.h — MiniSQL Week 7 B+ Tree 인덱스 인터페이스 계약 (MP1 확정).
 *
 * id 컬럼 (정수) → row_index (저장소 CSV 행 번호) 매핑 인덱스.
 *   - executor.c (정환) 가 WHERE id=? 에서 bptree_search 로 O(log n) 조회
 *   - storage.c  (민철) 가 INSERT 시 bptree_insert 로 (id, row_idx) 등록
 *   - bench/     (규태) 가 대량 데이터로 성능 측정
 *
 * 이 헤더는 MP1 머지 이후 **수정 금지**.
 * 시그니처 변경이 필요하면 팀 합의 후 별도 MP 에서만 갱신.
 */

#ifndef BPTREE_H
#define BPTREE_H

/* 불투명 구조체: 내부 레이아웃은 bptree.c 내부에서만 노출.
 * 호출자는 항상 포인터로만 사용. */
typedef struct BPTree BPTree;

/* bptree_create: 새 B+ 트리를 동적 할당해서 반환.
 *
 *   order — 한 노드가 가질 수 있는 자식 수의 상한 (>=3 권장).
 *           내부/리프 모두 동일한 order 를 사용.
 *
 * 반환: 성공 시 새 트리, 실패(메모리 부족) 시 NULL.
 * 호출자는 bptree_destroy() 로 해제 책임. */
BPTree *bptree_create(int order);

/* bptree_insert: (id, row_index) 매핑을 트리에 삽입.
 *
 *   tree      — bptree_create 로 만든 트리. NULL 금지.
 *   id        — 정수 키. 중복 삽입 시 동작은 구현 정의 (일단 덮어쓰기 가정).
 *   row_index — storage CSV 의 0-based 행 번호.
 *
 * 실패 조건 (메모리 부족 등) 은 일단 무시 — 이후 확장 시 int 반환으로 갱신. */
void bptree_insert(BPTree *tree, int id, int row_index);

/* bptree_search: id 키로 row_index 조회.
 *
 *   tree — NULL 금지.
 *   id   — 찾을 키.
 *
 * 반환: 찾으면 row_index (>=0), 못 찾으면 -1. */
int bptree_search(BPTree *tree, int id);

/* bptree_range: [from, to] 범위 (양끝 포함) 의 row_index 들을 out 에 채움.
 *
 *   tree    — NULL 금지.
 *   from,to — 키 범위. from <= to.
 *   out     — 결과 버퍼 (호출자 할당).
 *   max_out — out 의 원소 개수 상한.
 *
 * 반환: 실제로 out 에 채운 개수 (0..max_out).
 *       max_out 보다 많은 매칭이 있어도 그 이상은 채우지 않고 max_out 반환.
 *       Week 7 MVP 에서는 선택 구현 — 미구현 시 0 반환으로 둬도 OK. */
int bptree_range(BPTree *tree, int from, int to, int *out, int max_out);

/* bptree_print: 디버그용. 트리 구조를 stdout 에 보기 좋게 출력.
 * 구현 형식은 자유 (레벨별 들여쓰기 등). 실사용/테스트엔 영향 없음. */
void bptree_print(BPTree *tree);

/* bptree_destroy: 트리가 소유한 모든 노드 메모리 해제.
 * tree 자체도 free. 이후 tree 포인터는 무효. NULL 허용 (no-op). */
void bptree_destroy(BPTree *tree);

#endif /* BPTREE_H */
