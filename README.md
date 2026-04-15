# MiniSQL — Week 7: B+ Tree Index

> Week 6 SQL 처리기에 B+ 트리 인덱스 + 고정폭 바이너리 저장 레이어를 얹은 확장 프로젝트.
> C 언어, CSV/바이너리 혼합 저장, CLI + Web UI 인터페이스.

**요약 수치 (WSL 9p / Linux x86_64):**
- B+ 트리 단건 조회 **2.18M ops/s** (선형 대비 **1,842×**)
- `storage_insert` **32 ms → 3 µs / 건** (10,000×) — [INSERT 최적화 여정](#insert-성능-최적화-여정-32-ms--3-µs) 참조
- 1M 행 `WHERE id BETWEEN` SQL 질의 **5.6 s → 2.2 s** (고정폭 바이너리 fseek 경로)
- 웹 데모: 결제 로그 장애 구간 조회 시연 (`python3 web/server.py`)

---

## 이전 프로젝트에서 이어받은 것

| 파일 | 내용 |
|---|---|
| `src/parser.c` | SQL 토크나이저 + ParsedSQL 구조체 |
| `src/executor.c` | CREATE / INSERT / SELECT / UPDATE / DELETE 실행 |
| `src/storage.c` | CSV 파일 기반 테이블 저장/읽기, RowSet 인프라 |
| `include/types.h` | ParsedSQL, RowSet, WhereClause 등 핵심 타입 |

Week 6 단위 테스트 227개 전부 그대로 통과하는 것이 이번 주 출발 조건입니다.

---

## 진행 현황

- **Round 1 (MP1~MP4)** — ✅ main 배포 (PR #18)
  - B+ 트리 core, auto-id, executor `WHERE id = ?` 분기, 100만 건 벤치, valgrind 0
- **Round 2 (MP6~MP10)** — ✅ main 배포 (PR #27)
  - BETWEEN 실행 경로, DELETE/UPDATE 인덱스 동기화, 선형 vs 인덱스 비교 벤치, 웹 데모
- **Round 3 (perf + 바이너리)** — ✅ 진행 중
  - INSERT 10,000× 가속 (PR #28), CSV 1-pass rebuild + py 직접 생성 (PR #29), 고정폭 바이너리 저장

---

## 이번 주 목표

테이블에 레코드가 삽입될 때 `id`를 B+ 트리 인덱스에 자동 등록하고,  
`WHERE id = ?` 및 `WHERE id BETWEEN A AND B` SELECT에서 인덱스를 활용해 O(log n) / O(log n + k) 탐색을 수행합니다.

```sql
-- 이전과 동일한 SQL, 내부 동작만 달라짐
INSERT INTO users (name, age) VALUES ('Alice', 30);   -- id 자동 부여 + 인덱스 등록
SELECT * FROM users WHERE id = 1;                      -- B+ 트리 탐색 (O log n)
SELECT * FROM users WHERE name = 'Alice';              -- 선형 탐색 유지
```

---

## 구현 범위

### 필수
- `BPTree` 구조체 + `bptree_insert` / `bptree_search` / `bptree_destroy`
- Leaf split, Internal split, Root split (트리 성장)
- `storage_insert` 에 auto-increment id + `bptree_insert` 연동
- `executor.c` WHERE 컬럼이 `id` 일 때 `bptree_search` 분기
- 100만 건 삽입 후 id 검색 vs 필드 검색 벤치마크

### 추가 (차별점)
- Range query 지원: `WHERE id BETWEEN 100 AND 200` (리프 linked list 활용)
- `bptree_print()` — 삽입/split 과정 ASCII 시각화
- 레코드 수별 탐색 시간 측정 테이블 출력 (O(log n) vs O(n) 증명)
- **웹 데모 (보너스):** 결제/트랜잭션 로그 시연 — 정적 HTML + Python stdlib 중개 서버로 장애 구간 range query 시각화 (`web/`)

---

## 아키텍처

```
┌──────────────────────────── Client ────────────────────────────┐
│   CLI ($ ./sqlparser file.sql)                                 │
│   Web UI (http://localhost:8080)                               │
└────────────────┬───────────────────────────────┬───────────────┘
                 │ SQL 텍스트                     │ HTTP JSON
                 ▼                                ▼
        ┌─────────────────┐              ┌──────────────────┐
        │  parser.c       │              │  web/server.py   │
        │  tokenize+AST   │              │  subprocess      │
        │  BETWEEN 지원   │──────────┐   │  ./sqlparser 호출│
        └────────┬────────┘          │   └────────┬─────────┘
                 │ ParsedSQL         │            │
                 ▼                   │            │
        ┌─────────────────┐          │            │
        │  executor.c     │          │            │
        │  QUERY_SELECT → │          │            │
        │  storage_ensure │          │            │
        │  _index() lazy  │          │            │
        │  try_range →    │          │            │
        │  try_indexed →  │          │            │
        │  storage_select │          │            │
        └────────┬────────┘          │            │
                 │                   │            │
    ┌────────────┼───────────────────┘            │
    ▼            ▼                                ▼
┌──────────┐ ┌──────────────────────────────────────────────────┐
│ bptree.c │ │ storage.c                                        │
│ leaf+    │ │   auto-id + append_fp 캐시 + schema 캐시 +       │
│ internal │ │   path resolution 캐시 + BULK_INSERT_MODE +      │
│ split    │ │   storage_ensure_index (lazy rebuild)            │
│ range    │ │   ┌────────────────┬─────────────────────────┐   │
│ print    │ │   │ CSV fallback   │ 고정폭 BIN fast path    │   │
└────┬─────┘ │   │ (가변 길이)    │ (fseek O(K))            │   │
     │       │   └────────┬───────┴──────────────┬──────────┘   │
     ▼       └────────────┼──────────────────────┼──────────────┘
 index_registry           ▼                      ▼
 (테이블별 BPTree*)  data/tables/<t>.csv   data/tables/<t>.bin
                     data/schema/<t>.schema
```

## 파일 구조

```
.
├── include/
│   ├── types.h          ← ParsedSQL, RowSet, WhereClause, storage_* 선언
│   │                      (Round 2 에서 value_to + by_row_indices 추가)
│   ├── bptree.h         ← B+ 트리 공개 API (MP1 동결, 수정 금지)
│   └── index_registry.h ← 테이블 이름 → BPTree* 매핑
├── src/
│   ├── main.c           ← SQL 파일 읽기 → statement 분할 → execute
│   ├── parser.c         ← tokenize + AST + BETWEEN 파싱
│   ├── executor.c       ← QUERY_SELECT → lazy ensure_index → range/indexed/linear
│   ├── storage.c        ← CSV 기반 저장 + 고정폭 BIN 읽기 + 5가지 캐시
│   │                      (append FP, schema, path, meta, index rebuilt)
│   ├── bptree.c         ← B+ 트리 구현 (leaf/internal/root split)
│   ├── index_registry.c ← 다중 테이블 트리 관리
│   ├── ast_print.c / json_out.c / sql_format.c ← Week 6 그대로
├── tests/
│   ├── test_parser.c       ← BETWEEN 파싱 등 208 assertion
│   ├── test_executor.c     ← 인덱스/BETWEEN 분기 정합성
│   ├── test_bptree.c       ← 56 assertion
│   ├── test_storage_*.c    ← insert/delete/update + 인덱스 동기화
│   ├── test_index_registry.c
│   └── test_benchmark.c    ← 벤치 17 케이스
├── bench/
│   └── benchmark.c         ← 100만 건 INSERT/SEARCH/RANGE + 선형 vs 인덱스 비교
├── scripts/
│   └── gen_payments_fixture.py ← 결제 더미 CSV + 고정폭 BIN 동시 생성기
├── web/                    ← 웹 데모 (의존성 0)
│   ├── index.html          ← Porsche 오마주 UI + Chart.js
│   ├── app.js              ← /api/range, /api/compare 등 fetch
│   ├── cursor-trail.js
│   └── server.py           ← stdlib http.server + subprocess sqlparser 중개
├── data/                   ← .gitignore (로컬 전용)
│   ├── schema/<table>.schema
│   └── tables/<table>.csv  + <table>.bin (선택적 fast path)
├── .devcontainer/
├── .github/workflows/build.yml ← make + test + valgrind 자동
├── Makefile
├── CLAUDE.md / agent.md / claude_jiyong.md ← 팀 계약 문서
└── README.md
```

---

## B+ 트리 인터페이스

```c
// include/bptree.h

typedef struct BPTree BPTree;

BPTree *bptree_create(int order);
void    bptree_insert(BPTree *tree, int id, int row_index);
int     bptree_search(BPTree *tree, int id);          // row_index 반환, 없으면 -1
int     bptree_range(BPTree *tree, int from, int to,  // 추가 구현 시
                     int *out, int max_out);
void    bptree_print(BPTree *tree);                   // ASCII 시각화
void    bptree_destroy(BPTree *tree);
```

---

## 빌드 & 실행

```bash
make              # 전체 빌드
make test         # 단위 테스트 (Week 6 회귀 포함)
make bench        # 100만 건 성능 벤치마크
make valgrind     # 메모리 누수 검사
make clean

# 웹 데모 (보너스, 발표 시연용)
make              # sqlparser / benchmark 빌드 선행
python3 web/server.py      # → http://localhost:8080 접속

# 대용량 결제 로그 픽스처 (웹 데모 대용량 시연용, SQL INSERT 우회)
python3 scripts/gen_payments_fixture.py          # 100만 건
python3 scripts/gen_payments_fixture.py 10000000 # 1000만 건
# 첫 SELECT 시 sqlparser 가 CSV → B+ 트리 lazy rebuild 수행.
```

### 대용량 데이터 주입 전략

| 규모 | 방식 | 근거 |
|---|---|---|
| ≤ 200k | SQL `INSERT` + `BULK_INSERT_MODE=1` | 파싱 / executor 오버헤드 수용 가능 |
| > 200k | Python 직접 CSV + BIN 작성 (`scripts/gen_payments_fixture.py`) | sqlparser INSERT 파이프라인 우회로 1M=2.6s / 10M=24s |

웹 UI `/api/inject` 는 `count` 기준으로 자동 분기 (200k 경계).

---

## INSERT 성능 최적화 여정 (32 ms → 3 µs)

초기 웹 데모에서 100만 건 inject 가 60초 타임아웃에 2,716 건만 들어가던 상황 (32 ms/건).
아래 5 단계로 **10,000× 가속**.

| 단계 | 조치 | 효과 (10k INSERT) |
|---|---|---|
| 원인 파악 | `user+sys 0.7s`, `real 14.8s` — 93%가 I/O wait. `df -T`: `data/` 가 WSL 9p + `dirsync` | 원인 확정 |
| ① append FILE\* 캐시 | `fopen/fclose` per row → 프로세스 생애동안 hold-open + setvbuf 64KB. dirsync 비용을 atexit 1회로 압축 | 14.8s → 13.5s |
| ② schema 캐시 | `load_schema` (fopen + parse) 가 매 INSERT 호출 → 테이블당 1회만 로드 후 memcpy clone | 13.5s → 4.8s |
| ③ path resolution 캐시 | `build_schema_path` / `build_table_path` 의 legacy/nested fallback 최대 5 stat → 첫 호출만 | 4.8s → 1.4s |
| ④ user-id 경로 meta cache 통합 | 사용자가 `id` 명시한 INSERT 는 meta cache 미초기화 → `count_csv_rows` 매 호출 O(N²). 경로 무관 통합 | O(N²) 제거 |
| ⑤ `BULK_INSERT_MODE=1` | per-insert `fflush` 생략 (setvbuf 버퍼 가득 찰 때만 write). 대량 주입 전용 | 1.4s → **0.058s** |

**정리:** 병목은 9p 파일시스템의 **stat/fopen 당 ~0.3ms 블로킹**. 행당 5~6회 호출하던 것을 테이블당 1회로 줄이고, write 는 대용량 버퍼에 누적해 2회→1회로 압축.

### `storage_insert` 당 I/O 감소 상세
```
Before                              After
─────────────────────────           ─────────────────────────
load_schema fopen      1 stat       → 캐시 히트, 0 stat
                       1 read       → 0 read
build_schema_path      1~2 stat     → 캐시 히트, 0 stat
build_table_path       1~3 stat     → 캐시 히트, 0 stat
stat (cache validate)  1 stat       → append fp 살면 생략
append_csv_row fopen   1 stat+open  → 프로세스 생애 1회
write                  1 write      → 버퍼 64KB 차면 write
fclose                 1 sync       → atexit 1회만
─────────────────────────           ─────────────────────────
Total: ~6 stat + 4 write/sync       Total: ~0 stat + 1/64KB write

9p 라운드트립 0.3ms × 6 ≈ 1.8ms              ≈ 0
```

### 1M 건 최종 결과
| 모드 | 1M INSERT |
|---|---|
| 수정 전 | ~25분 (추정, timeout 자주) |
| 기본 (safe fflush) | **~138s** |
| `BULK_INSERT_MODE=1` | **2.8s** (357k ops/s) |
| Python 직접 CSV + BIN 작성 (sqlparser 우회) | **2.6s** |

---

## 고정폭 바이너리 저장 레이어

CSV 는 가변 길이라 "N번째 행" 접근이 O(N). 이 구조적 한계를 풀기 위해 같은 데이터를
**고정폭 바이너리 파일**로도 직렬화한다. 존재 여부로 opt-in (선택적 fast path).

### 컬럼별 고정 바이트 레이아웃 (little-endian)

| ColumnType | 바이트 | 인코딩 |
|---|---|---|
| `INT` | 4 | `int32_t` |
| `FLOAT` | 8 | `double` |
| `BOOLEAN` | 1 | `uint8_t` (0/1) |
| `VARCHAR` | 32 | `\0` 패딩 |
| `DATE` | 16 | `\0` 패딩 'YYYY-MM-DD' |
| `DATETIME` | 24 | `\0` 패딩 'YYYY-MM-DD HH:MM:SS' |

전체 행은 이들의 연접. 예: `payments (id INT, user_id INT, amount INT, status VARCHAR, created_at INT)`
→ 4+4+4+32+4 = **48 bytes / row**.

### 접근 패턴

```c
/* 기존 CSV: N번째 행을 찾으려면 처음부터 개행 세기 */
for (int i = 0; i < N; i++) read_csv_line(fp);        // O(N)

/* 바이너리: 정확한 offset 으로 직접 seek */
fseek(fp, row_idx * 48, SEEK_SET);                     // O(1)
fread(buf, 48, 1, fp);
```

### 트리거 조건

- `data/tables/<table>.bin` 파일이 존재하면 자동 활성화
- `storage_select_result_by_row_indices` 가 감지해서 fseek 경로로 분기
- `storage_ensure_index` 도 BIN 이 있으면 id 컬럼 offset 만 읽어서 트리 재구성 가속
- 파일이 없으면 기존 CSV 경로로 fallback (100% 호환)

### 생성

`scripts/gen_payments_fixture.py` 와 `web/server.py`의 py-direct inject 가
CSV 와 BIN 을 **동일 데이터로 동시에** 씀. 바이너리 포맷은 `src/storage.c` 의
`decode_binary_row` 와 1:1 대응.

### 성능 효과 (1M payments, `WHERE id BETWEEN 500000 AND 501500`)

| 단계 | 질의 소요 |
|---|---|
| CSV only (rebuild 2-pass, 전체 로드 후 필터) | 8.06 s |
| CSV only (rebuild 1-pass) | 5.77 s |
| **CSV + BIN** (rebuild via BIN + retrieval via fseek) | **2.22 s** |

- 주요 절감 구간: retrieval 시 CSV 전체 로드 → BIN fseek O(K)
- 추가 절감: rebuild 시 CSV line 파싱 → BIN 의 id offset 만 4바이트씩 읽기

### 한계 / 향후
- **schema 변경 시 BIN 재생성 필요** (현재 수동)
- INSERT 경로는 아직 BIN 갱신 안 함 (CSV append + BIN append 양면 쓰기 필요)
- 이번 구현은 read-only fast path. Demo 시나리오는 py 가 매번 전체 BIN 생성하는 방식.

---

## 역할 분담

| 담당 | 파트 |
|---|---|
| 지용 | `bptree.c` 코어 + `bptree.h` 인터페이스 확정 + 레포/Makefile + 머지 |
| 정환 | `executor.c` WHERE id 분기 + 기존 SQL 처리기 이식 검증 |
| 민철 | `storage.c` auto-increment + `bptree_insert` 연동 |
| 규태 | `bench/benchmark.c` + 더미 데이터 생성 + README |
| 규태 (보너스) | `web/` 웹 시연 UI (정적 HTML + Python stdlib 중개 서버) |

---

## 브랜치 전략

```
main
└── dev
    ├── feature/bptree-core       (지용)
    ├── feature/executor-index    (팀원 A)
    ├── feature/storage-autoid    (팀원 B)
    └── feature/benchmark         (팀원 C)
```

- `feature/* → dev` PR → 지용 리뷰 후 머지
- `dev → main` 은 최종 통합 시 1회
- `bptree.h` 인터페이스 확정(MP1) 전까지 팀원 작업 시작 X

---

## 머지 포인트

| 시점 | 내용 |
|---|---|
| MP1 | `bptree.h` 인터페이스 확정 → 전원 브랜치 생성 가능 |
| MP2 | `bptree.c` search + insert (split 없이) 완성 → B, C 실제 연결 |
| MP3 | split 로직 완성 + 전체 통합 빌드 통과 |
| MP4 | 100만 건 테스트 + valgrind 0 → dev → main 머지 |
| MP5 (선택) | `web/` 데모 PR 머지 — 본진 회귀 0 확인 후에만 |

---

## 커밋 컨벤션

```
feat:     새 기능
fix:      버그 수정
test:     테스트 추가/수정
refactor: 리팩토링
docs:     문서
chore:    설정, 환경
```

---

## PR 체크리스트

- `make` 빌드 경고 없음 (`-Wall -Wextra -Wpedantic`)
- Week 6 단위 테스트 회귀 0
- 본인 영역 새 테스트 추가
- `valgrind --leak-check=full` 누수 0
- `bptree.h` 인터페이스 계약 위반 없음

---

## 성능 목표 및 실측

### 1. `make bench` — 순수 B+ 트리 레벨 (N = 1,000,000, order = 128)

| 연산 | 건수 | 소요 | 처리량 |
|---|---|---|---|
| INSERT | 1,000,000 | 0.699 s | **1,431,003 ops/s** |
| SEARCH | 1,000,000 | 0.458 s | **2,182,830 ops/s** |
| RANGE (폭 100) | 1,000 회 | 0.001 s | **1,258,973 qps** |
| VERIFY | 1,000,000 / 1,000,000 | — | 100.0% |

### 2. 선형 vs B+ 트리 비교 (MP9, pure tree level)

| 방식 | 소요 | 처리량 | 배율 |
|---|---|---|---|
| **선형** flat array O(n) | 1.018 s | 982 qps | 1.0 × |
| **B+ 트리** O(log n) | 0.001 s | 1,809,509 qps | **1,842.9 ×** |

### 3. SQL 질의 (`WHERE id BETWEEN 500000 AND 501500` on 1M rows)

| 저장 | 질의 소요 | 구성 |
|---|---|---|
| CSV (rebuild 2-pass) | 8.06 s | 전체 CSV 로드 후 row_idx 필터 |
| CSV (rebuild 1-pass) | 5.77 s | scan_csv_meta 제거 |
| **CSV + 고정폭 BIN** | **2.22 s** | rebuild·retrieval 모두 BIN fseek |
| linear `status='FAIL'` 기준선 | 4.99 s | 인덱스 불가 |

- INSERT 평균 **0.70 µs/op**, SEARCH 평균 **0.46 µs/op**
- `valgrind --leak-check=full` leak 0 (CI 자동)

---

## 발표 시연 시나리오 — 결제/트랜잭션 로그 (보너스)

> **핵심 멘트:** *"장애 발생 시 특정 시간 구간의 트랜잭션 로그를 빠르게 조회해야 한다 — B+Tree range query 가 O(log n + k) 로 해결한다."*

**데이터 모델:**
```sql
CREATE TABLE payments (
    id INT, user_id INT, amount INT,
    status TEXT,        -- 'SUCCESS' | 'FAIL' | 'TIMEOUT'
    created_at INT      -- Unix timestamp
);
```

**UI 3 버튼 시나리오 (~3분):**
1. **[ 더미 주입 ]** — 10만~100만 건 결제 로그, 실패율 5% / 타임아웃 2%
2. **[ 장애 구간 조회 (range) ]** — `WHERE id BETWEEN A AND B` 로 특정 시간 구간 추출, 1ms 내 반환
3. **[ 선형 vs 인덱스 비교 ]** — 같은 범위를 선형 탐색으로도 돌려 Chart.js 막대그래프 2개 (400배 단축 시각화)

**발표자 스토리:** *"새벽 3시 결제 장애. 로그에서 3:00~3:15 구간만 뽑아야 한다. id 는 시간순 auto-increment 이므로 id 범위 = 시간 구간 proxy."*

---

## 데이터 플로우 (웹 → 디스크)

웹 데모는 **별도 DB 데몬이 없다**. 기존 CLI 바이너리 `./sqlparser` 를 매 요청마다 `subprocess` 로 띄우는 얇은 wrapper.

```
[브라우저]
  │  fetch POST /api/query | /api/inject | /api/bench
  ▼
[web/server.py]   ← http.server stdlib, CORS 불필요 (정적 파일도 같은 포트)
  │  subprocess.run(["./sqlparser", "--json", SQL])  or  ["./benchmark", ...]
  ▼
[./sqlparser]   ← C 바이너리, 요청마다 새 프로세스
  ├─ parser.c       : SQL 토큰화 + AST
  ├─ executor.c     : WHERE id=? → bptree_search / BETWEEN → bptree_range / else 선형
  ├─ storage.c      : CSV read/append, RowSet 구성
  └─ bptree.c       : 메모리 B+ 트리 (프로세스 수명 동안만 존재)
        │
        ▼
[디스크]
  data/schemas/<table>.schema    ← 컬럼 정의 (영속)
  data/tables/<table>.csv        ← 실제 레코드 (영속)
```

**`/api/inject` (더미 결제 100k 주입) 의 구체 흐름:**
1. `server.py:inject_payments()` 가 `INSERT INTO payments ...` SQL 문자열 대량 생성
2. `./sqlparser` 한 번 호출 → 해당 프로세스에서 CSV append + B+Tree 생성/누적
3. 프로세스 종료 → **B+Tree 소멸, CSV 만 남음**

---

## 영속화 & 캐싱 — 현재 상태와 한계

### ✅ 있는 것
| 대상 | 위치 | 범위 |
|---|---|---|
| **CSV 영속** | `data/tables/*.csv`, `data/schemas/*.schema` | 프로세스 간 영속 (디스크) |
| **`s_meta` 캐시** | `storage.c` — 테이블별 `next_id`, `next_row_idx` | 한 프로세스 내. 반복 INSERT 시 CSV 전체 재스캔 방지 |
| **`index_registry`** | `src/index_registry.c` — 테이블명 → `BPTree*` 매핑 | 한 프로세스 내. 같은 프로세스에서 여러 쿼리가 같은 트리 재사용 |
| **B+Tree 재구성** | `storage.c:rebuild_index()` — DELETE/UPDATE 성공 시 CSV 전체를 다시 읽어 트리 재구축 | 한 프로세스 내 정합성 보장 |

### ❌ 없는 것 (의도된 한계)
- **B+Tree 영속 X** — 메모리에만 존재. 프로세스 종료 시 소멸
- **프로세스 간 캐시 X** — subprocess 기반이라 각 `./sqlparser` 호출은 콜드 스타트
- **페이지 버퍼풀 X** — 디스크 I/O 는 매 쿼리마다 `fopen` → 전체 스캔/append

### ⚠️ 웹 데모 관점에서 중요한 함의
- `/api/inject` 호출 직후 **다른** `/api/query` 호출로 `WHERE id=?` 를 날리면 새 프로세스 → `index_registry` 가 비어 있음 → `executor_try_indexed_select` 가 NULL 리턴 → 선형 탐색 fallback
- 즉 웹 UI 의 "주입 후 id 조회" 는 현재 구조상 **인덱스 이점을 얻지 못한다**
- 벤치 차트(`/api/bench`) 가 빠른 건 `./benchmark` 바이너리가 **자기 프로세스 내에서** INSERT → SELECT 를 연속으로 수행하기 때문 (트리가 살아있는 동안 조회)

### 이번 프로젝트의 의도
이번 스프린트의 scope 는 **"B+Tree 자료구조 + SQL 실행 경로 통합"** 이고, 디스크 기반 DBMS 의 영속 인덱스는 범위 밖. "CSV = 영속 데이터, B+Tree = 세션 인덱스" 라는 이분법이 의도된 선택입니다.

### 확장 여지 (Q&A 대비)
- **인덱스 사이드카 파일**: `data/indexes/<table>.bpt` 로 트리를 덤프/로드 → 콜드 스타트 제거
- **상주 데몬 모드**: `sqlparser --daemon` + Unix socket → `server.py` 가 접속만 → 트리가 프로세스 수명만큼 산다
- **버퍼풀**: CSV 대신 고정 크기 페이지 파일 + LRU 버퍼풀 (PostgreSQL shared_buffers 계보)

---

## 메모리 할당 레이어 — CS:APP malloc lab 과의 차이

발표 포인트: **우리 프로젝트는 자체 allocator 를 구현하지 않고 libc 위에 올라가 있다.**

```
[ bptree.c / storage.c / executor.c ... ]   ← 애플리케이션 (B+Tree 노드, 스키마, RowSet)
              │  malloc / calloc / realloc / free
              ▼
[ glibc ptmalloc2 ]                          ← 청크 관리, bin/arena, free list
              │  brk() / mmap()
              ▼
[ Linux 커널 ]                                ← 실제 가상 메모리
```

| 항목 | CS:APP malloc lab (`mm_malloc`) | 이 프로젝트 |
|---|---|---|
| 할당 주체 | `mm_malloc` 을 직접 구현 | libc `malloc` 호출만 |
| 힙 확보 | `mem_sbrk` 로 받은 고정 힙 시뮬레이터 | ptmalloc2 가 `brk`/`mmap` 으로 동적 확보 |
| 블록 관리 | 헤더/풋터, implicit · explicit · segregated free list 를 직접 작성 | ptmalloc2 의 bin/arena 가 처리, 우리는 개입 X |
| 관심사 | "할당기 자체를 어떻게 만드는가" | "할당기 위에서 자료구조를 어떻게 설계하는가" |
| 코드 예시 | `place()`, `coalesce()`, `find_fit()` | `malloc(sizeof *node)` 한 줄 |

**즉, B+Tree 노드 하나도 결국 ptmalloc2 청크 위에 얹힌다.**
`src/bptree.c` 의 `leaf_create` / `internal_create` / `node_free` 는 노드당 `malloc` 3회 (구조체 + keys + children/row_indices) + 대응되는 `free` 3회로 끝나며, 풀이나 arena 를 따로 두지 않는다.

**설계 선택의 이유**
- 이번 스프린트의 초점은 **B+Tree 자료구조와 SQL 실행 경로 통합**이지 allocator 최적화가 아님
- 100만 건 벤치 (INSERT 0.75 µs/op, SEARCH 0.41 µs/op) 에서 ptmalloc2 가 이미 충분히 빠름 — 커스텀 풀로 바꿔도 체감 이득은 제한적
- valgrind 누수 0 을 지키려면 할당/해제 대칭만 정확하면 되고, libc 에 위임하는 쪽이 검증 부담이 작음

**확장 여지 (발표 Q&A 대비)**
- 노드 크기가 고정이므로 **slab/pool allocator** 로 바꾸면 캐시 친화적 배치 + `malloc` 호출 횟수 감소 가능
- 디스크 기반으로 확장하면 페이지 단위 버퍼풀이 필요 → 이때는 libc 대신 자체 메모리 매니저가 필수

---

## 이전 프로젝트

Week 6 SQL Parser → https://github.com/JYPark-Code/jungle_w6_mini_mysql_sql_parser