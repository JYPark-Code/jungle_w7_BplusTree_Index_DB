# Agent Guide — MiniSQL Week 7: B+ Tree Index

## 프로젝트 개요

C로 구현한 파일 기반 SQL 처리기(Week 6)에 B+ 트리 인덱스를 추가.  
CLI로 SQL 파일 입력 → 파싱 → 실행 → 파일 DB 저장/읽기.  
Week 6 단위 테스트 227개 회귀 0 유지가 출발 조건.

---

## 환경 세팅

```bash
git clone <repo_url>
# VSCode → "Reopen in Container"
gcc --version
make --version
make
make test    # 227 통과 확인
```

---

## 작업 목적

`INSERT` 시 자동 부여된 `id`를 B+ 트리에 등록하고,  
`WHERE id = ?` SELECT에서 O(log n) 탐색을 수행한다.  
기존 `WHERE 다른필드 = ?`는 선형 탐색 그대로 유지.

---

## 그라운드 룰

1. `include/types.h`, `include/bptree.h` **수정 금지**
2. Angular Commit Convention 준수
3. 기능 완성 후 AI에게 unit test 생성 위임 → 통과 확인 후 PR
4. 담당 파일 외 수정 금지 (병렬 작업 충돌 방지)
5. MP1 머지 전 본인 작업 시작 X

---

## 브랜치 전략

```
main
└── dev
    ├── feature/bptree-core       (지용)
    ├── feature/executor-index    (정환)
    ├── feature/storage-autoid    (민철)
    └── feature/benchmark         (규태)
```

```bash
# MP1 머지 후 본인 브랜치 시작
git fetch origin
git checkout dev
git pull origin dev
git checkout -b feature/<본인영역>

# 작업 → 커밋 → PR
make && make test
git add -A
git commit -m "feat(...): ..."
git push -u origin feature/<본인영역>
# GitHub에서 base=dev로 PR 생성
```

---

## 역할별 상세 작업

### 지용 — bptree.c 코어 + PM

**담당 파일:** `src/bptree.c`, `include/bptree.h` (MP1 확정), `Makefile`

**구현 순서:**
1. `BPTree`, `LeafNode`, `InternalNode` 구조체 정의
2. `bptree_search()` — 루트에서 리프까지 재귀 탐색
3. `bptree_insert()` — overflow 없는 단순 삽입
4. leaf split → internal split → root split

**핵심 구조체 스케치:**
```c
#define ORDER 4

typedef struct LeafNode {
    int keys[ORDER];
    int row_indices[ORDER];
    int num_keys;
    struct LeafNode *next;       // linked list (range scan용)
} LeafNode;

typedef struct InternalNode {
    int keys[ORDER];
    void *children[ORDER + 1];
    int num_keys;
    int is_leaf;                 // 공통 첫 필드 — 타입 판별용
} InternalNode;
```

**단위 테스트 (AI 생성 위임):**
- 삽입 후 search 반환값 검증
- split 후 트리 높이 증가 검증
- 1000건 삽입 후 전수 search 검증

---

### 정환 — executor.c WHERE id 분기

**담당 파일:** `src/executor.c`, `tests/test_executor.c` 보강

**작업 내용:**
- Week 6 SQL 처리기 이식 상태 확인 (make test 227 통과)
- SELECT 실행 경로에서 WHERE 컬럼명이 `"id"`인지 확인
- `bptree_search()` 분기 추가, 나머지는 기존 선형 탐색 유지

**구현 예시:**
```c
// executor.c SELECT 실행 부분
if (sql->where_count == 1 && strcmp(sql->where[0].column, "id") == 0) {
    int row_idx = bptree_search(g_tree, atoi(sql->where[0].value));
    if (row_idx == -1) { printf("(no result)\n"); return; }
    // row_idx로 직접 행 접근
} else {
    // 기존 storage_select() 호출 유지
    storage_select(sql->table, sql);
}
```

**단위 테스트 (AI 생성 위임):**
- `WHERE id = N` → bptree_search 경로 검증
- `WHERE name = 'X'` → 선형 탐색 경로 검증
- `WHERE id = 존재하지않는값` → 빈 결과 처리

---

### 민철 — storage.c auto-increment + 연동

**담당 파일:** `src/storage.c`, `tests/test_storage_insert.c` 보강

**작업 내용:**
- `storage_insert()` 내부에 `next_id` 카운터 추가 (전역 또는 파일 기반)
- 삽입된 행의 `row_index`를 `bptree_insert(g_tree, id, row_idx)`로 전달
- 프로그램 시작 시 기존 CSV에서 인덱스 rebuild (재시작 후에도 동작)

**구현 예시:**
```c
// storage.c
static int g_next_id = 1;

int storage_insert(const char *table, char **columns, char **values, int count) {
    int id = g_next_id++;
    // ... 기존 CSV 저장 로직 ...
    int row_idx = /* 저장된 행 번호 */;
    bptree_insert(g_tree, id, row_idx);
    return 0;
}
```

**단위 테스트 (AI 생성 위임):**
- 삽입 후 id 자동 부여 검증 (1, 2, 3 순서)
- `bptree_search(id)` → 올바른 row_index 반환 검증
- 기존 storage_insert 회귀 테스트 유지

---

### 규태 — benchmark.c + 더미 데이터

**담당 파일:** `bench/benchmark.c`, README 업데이트

**작업 내용:**
- 100만 건 INSERT SQL 루프 생성 + 실행
- `WHERE id = N` 랜덤 1000번 vs `WHERE name = 'X'` 랜덤 1000번 시간 측정
- `clock()`으로 측정, 결과 테이블 형태로 출력

**출력 예시:**
```
=== MiniSQL B+ Tree Index Benchmark ===
Records inserted : 1,000,000
Index search  (id)   : 1000 queries →   3.2 ms  (avg 0.003 ms)
Linear search (name) : 1000 queries → 412.7 ms  (avg 0.413 ms)
Speedup : ~128x
```

**AI에게 줄 프롬프트:**
> "C언어로 bench/benchmark.c 작성. executor.c의 실행 함수를 직접 호출해서
> INSERT 100만 건 수행 후, id 기준 랜덤 SELECT 1000회 vs name 기준 랜덤 SELECT 1000회를
> clock()으로 측정해서 테이블로 출력. `#include "../src/executor.c"` 방식 또는
> Makefile에서 링크."

**단위 테스트:** 벤치마크 실행 후 crash 없음 확인 (연기 테스트 OK)

---

## 머지 포인트

| MP | 시점 | 조건 |
|---|---|---|
| **MP1** | 11:00 | `bptree.h` 확정 + dev 브랜치 전원 생성 가능 |
| **MP2** | 13:00 | `bptree_search` + `bptree_insert` (split 없이) 동작 → 정환/민철 실연결 |
| **MP3** | 17:30 | split 완성 + 정환/민철 PR 머지 + 통합 빌드 227+ 통과 |
| **MP4** | 20:30 | 100만 건 테스트 + valgrind 0 + 규태 PR 머지 |
| **최종** | 21:00 | dev → main 머지 |

---

## 타임라인

| 시간 | 지용 | 정환 | 민철 | 규태 |
|---|---|---|---|---|
| 10:30–11:00 | 레포 세팅 + bptree.h → **MP1** | 환경 세팅 | 환경 세팅 | 환경 세팅 |
| 11:00–12:00 | 구조체 + search | executor 이식 확인 + 분기 설계 | auto-id 구현 | 더미 데이터 생성기 |
| 12:00–13:00 | 🍽 점심 | 🍽 점심 | 🍽 점심 | 🍽 점심 |
| 13:00–14:30 | insert (split 없이) → **MP2** | WHERE id 분기 구현 | bptree 연동 | INSERT 벤치 루프 |
| 14:30–16:00 | leaf split | 단위 테스트 + PR | 단위 테스트 + PR | SELECT 성능 측정 |
| 16:00–17:30 | internal/root split → **MP3** | 통합 지원 | 통합 지원 | 결과 출력 + PR |
| 17:30–18:00 | 전체 통합 빌드 검증 | — | — | — |
| 18:00–19:00 | 🍽 저녁 | 🍽 저녁 | 🍽 저녁 | 🍽 저녁 |
| 19:00–20:30 | valgrind + 버그 수정 → **MP4** | 버그 수정 지원 | 버그 수정 지원 | README 업데이트 |
| 20:30–21:00 | dev → main 머지 + 발표 준비 | 리허설 | 리허설 | 리허설 |
| 21:00~ | (2차 리팩토링 예비) | — | — | — |

---

## PR 체크리스트

- [ ] `make` 빌드 경고 없음 (`-Wall -Wextra -Wpedantic`)
- [ ] Week 6 회귀 0
- [ ] 새 단위 테스트 추가
- [ ] `valgrind --leak-check=full` 누수 0
- [ ] `types.h`, `bptree.h` 변경 없음
- [ ] 담당 파일 외 변경 없음
- [ ] Angular 커밋 컨벤션 준수

---

## 커밋 컨벤션

```
feat(scope):  새 기능
fix(scope):   버그 수정
test(scope):  테스트
refactor:     리팩토링
docs:         문서
chore:        설정
```

scope 예시: `bptree`, `executor`, `storage`, `bench`, `makefile`

---

## 파일 소유권 (충돌 방지)

| 파일 | 소유자 | 타인 수정 |
|---|---|---|
| `src/bptree.c` | 지용 | ❌ |
| `include/bptree.h` | 지용 (MP1 확정) | ❌ |
| `src/executor.c` | 정환 | ❌ |
| `src/storage.c` | 민철 | ❌ |
| `bench/benchmark.c` | 규태 | ❌ |
| `include/types.h` | 전원 수정 금지 | ❌ |
| `Makefile` | 지용 (각자 필요시 PR로 요청) | PR 경유 |

---

## 2차 리팩토링 (21:00~ 예비)

시간이 남으면 아래 순서로 진행:

1. Range query — `WHERE id BETWEEN A AND B` (리프 linked list 활용)
2. `bptree_print()` ASCII 시각화
3. 레코드 수별 탐색 시간 측정 테이블 (10만/50만/100만)

---

## FAQ

**Q. MP2 전에 정환/민철은 bptree 함수를 어떻게 쓰나요?**  
A. stub 함수로 컴파일만 되게 해두세요. `bptree_search()` stub은 항상 -1 반환.

**Q. g_tree(전역 트리 포인터)는 누가 선언하나요?**  
A. `main.c`에서 `BPTree *g_tree;`로 선언, `extern`으로 각 파일에서 참조. MP1에 같이 합의.

**Q. 막히면?**  
A. 1시간 이상 혼자 붙잡지 말고 즉시 지용에게. PM이 vibe coding으로 지원.
