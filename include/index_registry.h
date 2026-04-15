/* index_registry.h — 테이블 이름 → BPTree* 공용 레지스트리.
 *
 * Week 7 통합 계약:
 *   storage.c (민철) 는 INSERT 시 해당 테이블의 BPTree 를 얻어 bptree_insert.
 *   executor.c (정환) 는 WHERE id=? SELECT 에서 동일 BPTree 를 얻어 bptree_search.
 *   둘이 같은 인스턴스를 공유하게 하는 유일한 접점.
 *
 * 구현은 테이블 이름 고정 배열 (MAX_TABLES) 기반. 스프린트 범위에선 충분.
 * 스레드 안전성 보장 없음 (현재 코드는 싱글 스레드).
 */

#ifndef INDEX_REGISTRY_H
#define INDEX_REGISTRY_H

#include "bptree.h"

/* 존재하면 해당 테이블의 BPTree 반환. 없으면 NULL. */
BPTree *index_registry_get(const char *table);

/* 존재하면 기존 트리 반환, 없으면 order 로 새 트리 생성해서 등록 후 반환.
 * 등록 실패 (메모리 부족 / 테이블 슬롯 한계 초과) 시 NULL.
 * order 는 bptree_create 규약 그대로 (>=3). */
BPTree *index_registry_get_or_create(const char *table, int order);

/* 모든 등록된 트리 해제 + 레지스트리 비우기.
 * 프로세스 종료 시 한 번 호출하면 valgrind 에서 누수 0 유지.
 * 재호출 안전 (두 번째 호출은 no-op). */
void index_registry_destroy_all(void);

#endif /* INDEX_REGISTRY_H */
