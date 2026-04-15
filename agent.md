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

## 그라운드 룰 (Round 2 개정)

1. `include/bptree.h` **수정 금지**
   - `include/types.h` 는 **PM(지용) 단독 PR로만** 수정. 팀원은 건드리지 않음.
2. Angular Commit Convention 준수
3. 기능 완성 후 AI에게 unit test 생성 위임 → 통과 확인 후 PR
4. 담당 파일 외 수정 지양. 단 **성능/기능상 불가피 시 허용** → 같은 파일에 PR 2개 오면 **PM이 Mix merge로 정리**
   - 특히 `src/storage.c`, `src/executor.c` 는 공동 편집 허용
5. 선행 블로커(Phase 1) PR 머지 전 의존 작업 시작 X

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

### 규태 (보너스) — web/ 웹 시연 UI

**담당 파일:** `web/index.html`, `web/app.js`, `web/server.py` (전부 신설)
**브랜치:** `feature/web-demo` (MP4 머지 후 생성)
**전제:** 본진 벤치마크 PR 머지 완료 후에만 착수.

**작업 내용:**
- `server.py` — `http.server` stdlib 기반 중개 서버
  - `GET /` → `web/index.html`, `GET /app.js` → 정적 서빙
  - `POST /api/query` → body 의 SQL 을 `./sqlparser --json` subprocess 로 실행, stdout 을 JSON 으로 그대로 전달
  - `POST /api/bench` → `./benchmark` 실행, stdout 파싱해 `{insert_ops, search_ops, range_qps}` JSON 반환
- `index.html` — SQL 입력 `<textarea>` + 결과 `<table>` + 벤치 `<canvas>` 3영역
- `app.js` — `fetch('/api/query')`, `fetch('/api/bench')` 호출 + Chart.js CDN 으로 막대그래프

**스택 제약 (의존성 0):**
- npm / node / webpack 사용 금지
- Python 도 stdlib 만 (Flask, FastAPI 등 외부 패키지 X)
- Chart.js 는 CDN `<script>` 태그 한 줄로만

**금지 사항:**
- `include/*.h`, `src/*.c`, `tests/*.c`, `bench/*.c` 전부 **수정 금지** — 웹 레이어에서만 작업
- `Makefile` 수정이 필요하면 지용에게 요청

**단위 테스트 (AI 생성 위임):**
- `server.py` 의 SQL 파싱 / 벤치 stdout 파싱 함수는 `unittest` 로 커버
- 브라우저 동작은 수동 시나리오 체크리스트로 대체 OK

**완료 기준 (MP5, 선택):**
- `python3 web/server.py` 로컬 실행 → 브라우저에서 아래 "결제/트랜잭션 로그 시연" 3 시나리오 동작
- 기존 `make test` / `make bench` 회귀 0

**시연 시나리오 — 결제/트랜잭션 로그 (발표 메인 컨텐츠):**

> 발표용 핵심 멘트: **"장애 발생 시 특정 시간 구간의 트랜잭션 로그를 빠르게 조회해야 한다 — B+Tree range query 가 O(log n + k) 로 해결한다."**

데이터 모델:
```sql
CREATE TABLE payments (
    id INT, user_id INT, amount INT,
    status TEXT,       -- 'SUCCESS' | 'FAIL' | 'TIMEOUT'
    created_at INT     -- Unix timestamp
);
```

UI 버튼 3개 (최소 구성):
1. **[ 더미 주입 ]** — 10만~100만 건, 실패율 5% / 타임아웃 2% 섞어서 생성
2. **[ 장애 구간 조회 (range) ]** — `WHERE id BETWEEN A AND B` 로 특정 시간 구간 로그만 추출, 1ms 내 반환 표시
3. **[ 선형 vs 인덱스 비교 ]** — 같은 범위를 선형 탐색으로도 돌려서 Chart.js 막대그래프 2개, "400배 단축" 시각화

발표자가 말할 스토리: *"새벽 3시 결제 시스템 장애. 로그에서 3:00~3:15 구간만 빠르게 뽑아야 한다. id 는 시간순 auto-increment 이므로 id 범위 = 시간 구간 proxy 로 사용."*

**JSON 스키마 계약 (FE 깨짐 방지):**
- 착수 시점의 `./sqlparser --json` 출력 스펙을 `web/README.md` 에 박제
- 정환/민철 PR 머지 후 스키마 diff 발생 시 FE 파서만 수정
- 스키마 변경은 정환(executor/json_out 담당) 과 규태(FE 담당) 합의 후에만

---

## 머지 포인트

| MP | 시점 | 조건 |
|---|---|---|
| **MP1** | 11:00 | `bptree.h` 확정 + dev 브랜치 전원 생성 가능 |
| **MP2** | 13:00 | `bptree_search` + `bptree_insert` (split 없이) 동작 → 정환/민철 실연결 |
| **MP3** | 17:30 | split 완성 + 정환/민철 PR 머지 + 통합 빌드 227+ 통과 |
| **MP4** | 20:30 | 100만 건 테스트 + valgrind 0 + 규태 PR 머지 |
| **최종** | 21:00 | dev → main 머지 **(Round 1 완료, PR #18)** |
| **MP5** (선택) | 발표 전 | 규태 `feature/web-demo` PR 머지 — 본진 영향 0 확인 후에만 |

### Round 2 (2026-04-15~)
| MP | 조건 |
|---|---|
| **MP6** | Phase 1 — `include/types.h` (`WhereClause.value_to` + `storage_select_result_by_row_indices`) + parser BETWEEN + 테스트 머지 (지용 단독) |
| **MP7** | 정환 PR — BETWEEN 실행 경로(`bptree_range` 연결) E2E 동작 |
| **MP8** | 민철 PR — DELETE/UPDATE 인덱스 동기화 (rebuild 방식) |
| **MP9** | 지용 Mix merge — 정환+민철 통합 + 선형 vs 인덱스 비교 벤치 README 반영 |
| **MP10** | 규태 MP5 머지 → dev → main 최종 |

---

## Round 2 역할별 지시 (2026-04-15~)

### 정환 — BETWEEN 실행 경로 (`feature/executor-between`)

**선행 조건:** Phase 1 PR(지용) 머지 대기 → `include/types.h` 에 `WhereClause.value_to` 와 `storage_select_result_by_row_indices` 선언이 들어온 후 착수.

**작업 범위:**
- `src/executor.c`: `executor_try_range_select` 추가 — `where.op == "BETWEEN"` + `column == "id"` 감지 시 `bptree_range()` 로 id 배열 획득 → `storage_select_result_by_row_indices()` 호출 → `print_rowset`
- 기존 `WHERE id = ?` 경로(`executor_try_indexed_select`)는 그대로, 순서: range 먼저 시도 → fallback 으로 indexed → fallback 으로 storage_select
- `tests/test_executor.c`: `BETWEEN` 정상/경계/미존재 범위 3건 이상
- `src/storage.c` 에 `storage_select_result_by_row_indices` 구현체 추가 가능 (민철과 공동 편집 허용 — Mix merge 예정)

**에지 케이스:** `from > to` 정규화 / id 외 컬럼은 즉시 fallback / where_count != 1 fallback

### 민철 — DELETE/UPDATE 인덱스 동기화 (`feature/storage-index-sync`)

**선행 조건:** 없음 — 바로 착수 가능. 단 정환 PR 과 `src/storage.c` 영역 겹침 예상이라 개별 PR 후 PM Mix merge.

**작업 범위:**
- `src/storage.c`:
  - `storage_delete` 성공 후 해당 테이블의 B+ 트리 전체 rebuild (CSV 재스캔 → 각 행 (id, row_idx) 로 `bptree_insert`). 기존 트리는 `bptree_destroy` + `index_registry_get_or_create` 재생성.
  - `storage_update` 가 id 컬럼을 변경하는 경우도 동일하게 rebuild. id 불변이면 skip.
  - `storage_meta_cache` 무효화 포함 (next_id / next_row_idx 재계산)
- `tests/test_storage_delete.c`, `tests/test_storage_update.c`: DELETE/UPDATE 후 `bptree_search(id)` 가 올바른 row_idx 반환하는지 / 사라진 id 는 -1 인지

**대안 (보너스, 여력 있으면):** `bptree_delete(tree, id)` 를 `src/bptree.c` 에 추가 (지용 영역이라 사전 합의 필요 — 1차는 rebuild 로 안전하게)

### 지용 — Phase 1 선행 + 비교 벤치 + Mix merge

**PR 1 (Phase 1):**
- `include/types.h` 확장, `src/parser.c` BETWEEN 파싱, `tests/test_parser.c` 추가

**PR 2 (비교 벤치):**
- `bench/benchmark.c` 에 선형 탐색 루프 추가 — 같은 N 건에서 "name 컬럼 선형 탐색" vs "id 컬럼 B+ 트리" 시간 비교 출력
- `README.md` 성능 표에 "선형 vs 인덱스 배율" 컬럼 추가

**PR 3 (Mix merge):**
- 정환 PR + 민철 PR 받아 Mix merge 브랜치 → dev

### 규태 — MP5 웹 데모 (`feature/web-demo`)

**변경 없음** — Round 1 기준 지시서(`CLAUDE.md` 웹 데모 섹션) 그대로. Round 2 본진 작업과 독립. Phase 1 머지 후 BETWEEN 이 실제로 SQL 로 뚫리면 시연 임팩트가 커짐.

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

## 파일 소유권 (Round 2 개정)

| 파일 | 소유자 | 타인 수정 |
|---|---|---|
| `src/bptree.c` | 지용 | ❌ |
| `include/bptree.h` | 지용 (수정 금지) | ❌ |
| `src/executor.c` | 정환 (1차), **공동 편집 허용** | 🟢 Mix merge |
| `src/storage.c` | 민철 (1차), **공동 편집 허용** | 🟢 Mix merge |
| `bench/benchmark.c` | 규태 (1차) / 지용 (비교 벤치 추가) | 🟡 공유 |
| `web/` (전체) | 규태 (MP5) | ❌ |
| `src/json_out.c` | 정환 (출력 스키마 변경 시 규태에 공유) | 🟡 |
| `include/types.h` | **지용 단독 PR로만 수정** | ❌ |
| `src/parser.c` | 지용 | ❌ |
| `Makefile` | 지용 | PR 경유 |

**Round 2 공동 편집 규칙:** `executor.c` / `storage.c` 는 정환·민철이 동시에 수정 가능. 각자 PR 을 올리면 PM(지용)이 Mix merge 로 정리한다. 같은 함수를 건드리지 않도록 PR 본문에 건드린 함수명을 명시할 것.

---

## 2차 리팩토링 — Round 2 (공식화)

Round 1 (MP1~MP4) 달성 후 아래 작업을 공식 Round 2 로 진행 (상단 "Round 2 역할별 지시" 참고):

1. ✅ Range query — `WHERE id BETWEEN A AND B` (정환, MP7)
2. ✅ DELETE/UPDATE 인덱스 동기화 (민철, MP8)
3. ✅ 선형 vs 인덱스 비교 벤치 (지용, MP9)
4. (여력) `bptree_print()` ASCII 시각화 demo, 레코드 수별 탐색 시간 테이블

---

## FAQ

**Q. MP2 전에 정환/민철은 bptree 함수를 어떻게 쓰나요?**  
A. stub 함수로 컴파일만 되게 해두세요. `bptree_search()` stub은 항상 -1 반환.

**Q. g_tree(전역 트리 포인터)는 누가 선언하나요?**  
A. `main.c`에서 `BPTree *g_tree;`로 선언, `extern`으로 각 파일에서 참조. MP1에 같이 합의.

**Q. 막히면?**  
A. 1시간 이상 혼자 붙잡지 말고 즉시 지용에게. PM이 vibe coding으로 지원.
