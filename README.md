# MiniSQL — Week 7: B+ Tree Index

> Week 6 SQL 처리기에 B+ 트리 인덱스를 얹은 확장 프로젝트.  
> C 언어, 메모리 기반, CLI 인터페이스.

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

- **Round 1 (MP1~MP4)** — ✅ 2026-04-15 main 배포 (PR #18)
  - B+ 트리 core, auto-id, executor `WHERE id = ?` 분기, 100만 건 벤치, valgrind 0
- **Round 2 (MP6~MP10)** — 🔄 진행 중
  - BETWEEN 실행 경로, DELETE/UPDATE 인덱스 동기화, 선형 vs 인덱스 비교 벤치, 웹 데모(MP5)

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

## 파일 구조

```
.
├── include/
│   ├── types.h          ← Week 6 그대로 (RowSet, ParsedSQL 등)
│   └── bptree.h         ← NEW: B+ 트리 API 선언
├── src/
│   ├── main.c
│   ├── parser.c         ← 변경 없음
│   ├── ast_print.c      ← 변경 없음
│   ├── json_out.c       ← 변경 없음
│   ├── sql_format.c     ← 변경 없음
│   ├── executor.c       ← WHERE id=? 분기 추가
│   ├── storage.c        ← auto-increment id + bptree_insert 연동
│   └── bptree.c         ← NEW: B+ 트리 구현체
├── tests/
│   ├── test_parser.c
│   ├── test_executor.c
│   ├── test_storage_*.c ← Week 6 그대로
│   └── test_bptree.c    ← NEW: B+ 트리 단위 테스트
├── bench/
│   └── benchmark.c      ← NEW: 100만 건 성능 측정
├── web/                 ← NEW (보너스): 발표 시연용 웹 UI
│   ├── index.html       ← SQL 입력창 + 결과 테이블 + 벤치 차트
│   ├── app.js           ← fetch 로 /api/query, /api/bench 호출
│   └── server.py        ← http.server 중개 (stdlib만, 의존성 0)
├── .devcontainer/
├── .github/workflows/build.yml
├── Makefile
├── CLAUDE.md
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
python3 web/server.py      # → http://localhost:8000 접속
```

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

## 성능 목표 및 실측 (MP4)

`make bench` 실측 (N = 1,000,000, order = 128, Linux x86_64):

| 연산 | 건수 | 소요 | 처리량 |
|---|---|---|---|
| INSERT | 1,000,000 | 0.755 s | **1,325,300 ops/s** |
| SEARCH | 1,000,000 | 0.407 s | **2,455,631 ops/s** |
| RANGE (폭 100) | 1,000 회 | 0.001 s | **1,110,769 qps** (hits 100,000) |
| VERIFY | 1,000,000 / 1,000,000 | — | 100.0 % 조회 성공 |

- INSERT 평균 **0.75 µs/op**, SEARCH 평균 **0.41 µs/op** — 100만 건 규모에서 단건 조회가 1µs 미만
- `valgrind --leak-check=full` leak 0 (CI 자동 검증)
- MP4 완료 기준(100만 건 + valgrind 0) 충족

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

## 이전 프로젝트

Week 6 SQL Parser → https://github.com/JYPark-Code/jungle_w6_mini_mysql_sql_parser