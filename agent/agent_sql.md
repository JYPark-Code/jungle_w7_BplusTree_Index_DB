# Agent Guide — MiniSQL

## 프로젝트 개요
C로 구현한 파일 기반 SQL 처리기.
CLI로 SQL 파일을 입력받아 파싱 → 실행 → 파일 DB에 저장/읽기.
1주차 (1일) 완료 → **2주차 Phase 1** 진행 중.

---

## 환경 세팅
1. 레포 클론
   `git clone <repo_url>`
2. VSCode 에서 열기
3. 좌하단 "Reopen in Container" 클릭
4. 완료 후 터미널에서 확인
   ```
   gcc --version
   make --version
   make
   make test    # 201 통과
   ```

---

# 🔄 2주차 Phase 1 — 인터페이스 리팩토링

## 작업 목적
1주차 storage 의 가장 큰 약점 해소: **결과를 stdout 으로만 print 하고 데이터로 반환 못 함**.
이걸 풀어주면 향후 B+트리 / JOIN / 집계 / subquery 모든 길이 열린다.
**Phase 1 은 subquery 자체는 만들지 않는다.** 인터페이스만 정리.

## 브랜치 전략 (Phase 1)

```
main
└── dev2
    ├── feature/p1-interface-contract  (지용 — MP1 먼저)
    ├── feature/p1-rowset              (석제)
    ├── feature/p1-parser-stop-set     (지용)
    └── feature/p1-compound-where      (원우)
```

`main` 과 `dev2` 는 1주차와 동일하게 **브랜치 보호** 적용 (직접 push 차단, admin 만 우회).
모든 변경은 `feature/* → dev2 → main` 순서로 PR 을 거친다.

### dev2 브랜치 생성 (PM)
```bash
git checkout main
git pull origin main
git checkout -b dev2
git push -u origin dev2
```

### 본인 feature 브랜치 시작
```bash
git fetch origin
git checkout dev2
git pull origin dev2
git checkout -b feature/p1-<본인영역>
```

⚠ **MP1 (지용의 인터페이스 계약 PR) 머지 전엔 본인 작업 시작 X.**
MP1 머지 후 `git pull origin dev2` 로 새 `types.h` 받아온 뒤 시작.

---

## 역할별 담당 (3명)

### 🅐 지용 — RowSet 인프라 + 집계 함수 실행
**목표:**
1. storage 가 결과를 메모리 데이터로 반환하는 새 함수 도입
2. 집계 함수 (SUM/AVG/MIN/MAX) 평가 (COUNT(*) 는 1주차에 이미 동작)

**브랜치:** `feature/p1-rowset`

**작업 파일:**
- `include/types.h` — RowSet 구조체 + 새 함수 선언 (MP1 에 같이 들어감)
- `src/storage.c` — `storage_select_result`, `print_rowset`, `rowset_free` 신설
- `src/storage.c` — 기존 `storage_select` 를 wrapper 로 리팩토링
- `src/storage.c` — `print_selection` / `is_count_star` 의 함수 호출형 컬럼 분기를
  SUM/AVG/MIN/MAX 로 확장. 결과는 단일 행 RowSet
- `tests/test_storage_select_result.c` — 새 단위 테스트 + Makefile 통합

**핵심 동작 1 — RowSet:**
```c
RowSet *rs = NULL;
storage_select_result("users", sql, &rs);
// rs->row_count, rs->col_count, rs->col_names, rs->rows[i][j]
// 메모리에서 직접 검증
rowset_free(rs);
```

**기존 `storage_select` 는:**
```c
int storage_select(const char *table, ParsedSQL *sql) {
    RowSet *rs = NULL;
    int status = storage_select_result(table, sql, &rs);
    if (status == 0) print_rowset(stdout, rs);
    rowset_free(rs);
    return status;
}
```
→ **외부 동작 변화 0**. 모든 1주차 테스트 통과해야 함.

**핵심 동작 2 — 집계 함수:**
```sql
SELECT COUNT(*) FROM users;        -- ✅ 1주차에 이미 동작
SELECT SUM(price) FROM orders;     -- 🆕 Phase 1
SELECT AVG(age) FROM users;        -- 🆕
SELECT MIN(joined) FROM users;     -- 🆕
SELECT MAX(score) FROM exams;      -- 🆕
```

결과 RowSet:
```c
RowSet { col_names: ["SUM(price)"], rows: [["12345"]], row_count: 1, col_count: 1 }
```

**구현 가이드:**
- 1주차 `is_count_star()` 같은 검사 함수를 `is_aggregate_call()` 로 일반화
  → 함수 이름 (COUNT/SUM/AVG/MIN/MAX) 과 인자 컬럼 추출
- WHERE / ORDER BY / LIMIT 모두 통과한 행들 위에서 집계 계산
- 타입별 처리: SUM/AVG → INT/FLOAT 만, MIN/MAX → 모든 타입 (문자열 비교 OK)
- DATE 컬럼의 MIN/MAX 는 문자열 비교로 동작 (YYYY-MM-DD 정렬)
- BOOLEAN/VARCHAR 에 SUM/AVG 시도하면 친절한 에러

**필수 검증:**
- [ ] make 빌드 무경고
- [ ] make test 회귀 0 (1주차 201 통과 그대로)
- [ ] 새 RowSet 단위 테스트 추가
- [ ] 5종 집계 함수 단위 테스트 (COUNT/SUM/AVG/MIN/MAX)
- [ ] valgrind 누수 0

### 🅑 석제 — Parser stop set + N-ary WHERE + 집계 컬럼 회귀
**목표:**
1. `parse_select` 가 stop set (`)`, `;` 등) 을 만나면 멈추도록 → 향후 subquery / 괄호 그룹화 대비
2. `parse_where` 를 N-ary 로 확장 (현재 1~2 조건 → N개)
3. 집계 함수 컬럼 (`SUM(col)`, `AVG(col)` 등) 이 컬럼 자리에서 정상 토큰 결합되는지 회귀 검증
   (1주차 PR #19 의 COUNT(*) 핫픽스 메커니즘이 이미 함수 호출형을 받음 — 같은 길로 SUM/AVG/MIN/MAX 도 동작)

**브랜치:** `feature/p1-parser-stop-set`

**작업 파일:**
- `src/parser.c` — `parse_select`, `parse_where` 확장
- `src/ast_print.c` / `src/json_out.c` / `src/sql_format.c` — N-ary 출력 갱신
- `include/types.h` — 지용 MP1 에 결합자 배열 같이 들어감 (사전 협의 필수)
- `tests/test_parser.c` — 3개 이상 조건, 혼합 결합, 집계 컬럼 파싱 케이스

**현재 한계:**
```c
WHERE a = 1 AND b = 2          // ✅ 1주차 OK
WHERE a = 1 AND b = 2 AND c = 3 // ❌ 1주차 미지원 (2개까지)
```

**Phase 1 후:**
```c
WHERE a = 1 AND b = 2 AND c = 3                    // ✅
WHERE a = 1 OR b = 2 OR c = 3                      // ✅
WHERE a = 1 AND b = 2 OR c = 3                     // ✅ (왼→오 평가, 그룹화 X)
```

**그룹화 (괄호) 는 Phase 2 이후.** Phase 1 은 평면 N-ary 까지.

**필수 검증:**
- [ ] make 빌드 무경고
- [ ] 1주차 단위 테스트 회귀 0
- [ ] 새 N-ary 단위 테스트
- [ ] valgrind 누수 0

### 🅒 원우 — UPDATE/DELETE 시그니처 통일 + 복합 WHERE 평가
**목표:**
1. `storage_delete` / `storage_update` 시그니처를 SELECT 와 동일 패턴 (`ParsedSQL*` 한 인자) 으로 통일
2. 석제의 N-ary WHERE (결합자 배열) 가 정상 평가

**브랜치:** `feature/p1-compound-where`

**시그니처 변경:**
```c
// 기존 (1주차)
int storage_delete(const char *table, WhereClause *where, int where_count);
int storage_update(const char *table, SetClause *set, int set_count,
                   WhereClause *where, int where_count);

// Phase 1 후 — SELECT 와 동일 (ParsedSQL* 한 인자)
int storage_delete(const char *table, ParsedSQL *sql);
int storage_update(const char *table, ParsedSQL *sql);
```

**시그니처 변경 사유:**
- N-ary WHERE 결합자 배열 (`sql->where_links`) 에 접근하려면 ParsedSQL 전체가 필요
- SELECT 와 동일 패턴 → API 일관성, 코드 간결
- 호출부 (`executor.c`) 도 같이 변경: `storage_delete(sql->table, sql)`

**작업 파일:**
- `src/storage.c` — `storage_update`, `storage_delete` 본문 + N-ary WHERE 평가
  - 공용 `evaluate_where_with_links()` helper 도입 권장 (SELECT 도 같이 쓰면 좋음)
- `src/executor.c` — case 본문에서 호출 인자 변경 (한 줄씩)
- `include/types.h` — 시그니처 변경 (지용 MP1 에 같이 들어감)
- `tests/test_storage_delete.c` — 3개 이상 조건 케이스 추가 + 기존 케이스의 호출 인자 갱신
- `tests/test_storage_update.c` — 동상

**현재 한계:**
```c
DELETE FROM users WHERE age > 20 AND name = 'bob'                    // ✅ 1주차 OK
DELETE FROM users WHERE age > 20 AND name = 'bob' AND city = 'Seoul' // ❌ 미지원
DELETE FROM users WHERE age > 20 AND name = 'bob' OR city = 'Seoul'  // ❌ 단일 결합자만
```

**Phase 1 후:** 위 모두 정상 동작.

**필수 검증:**
- [ ] make 빌드 무경고
- [ ] 기존 48 storage 단위 테스트 회귀 0 (호출 인자만 갱신)
- [ ] 새 복합 WHERE 단위 테스트 (N개 조건, 혼합 결합)
- [ ] valgrind 누수 0

---

## 마일스톤 (Phase 1) — ✅ 모두 완료

| 마일스톤 | 기준 | 담당 | 상태 |
|----------|------|------|------|
| **M1** | dev2 브랜치 생성 + 본인 feature 브랜치 분기 | PM | ✅ |
| **M2** | MP1 (인터페이스 계약 PR) 머지 → 본인 작업 시작 | 지용 → 전원 | ✅ |
| **M3** | 본인 영역 1차 구현 동작 + 단위 테스트 1~2 개 | 셋 다 | ✅ |
| **M4** | 본인 영역 모든 단위 테스트 통과 + valgrind 0 + PR 제출 | 셋 다 | ✅ |
| **M5** | PM 리뷰 통과 + dev2 머지 | 셋 다 | ✅ |
| **M6** | 통합 (dev2 에서 모든 PR 머지된 상태) + 회귀 0 | 전원 | ✅ |

---

## 머지 포인트 (Phase 1) — ✅ 모두 완료

| 머지 포인트 | 내용 | 담당 | 상태 | PR |
|-------------|------|------|------|-----|
| **MP1** | 인터페이스 계약 (types.h) PR → dev2 머지 | 지용 | ✅ | #29 |
| **MP2** | 각 영역 PR 머지 | 셋 다 | ✅ | #30 / #32 / #33 |
| **MP3** | dev2 통합 + 회귀 검증 | PM | ✅ | (227 통과) |
| **MP4** | dev2 → main 머지 | PM | ✅ | #34 |
| **MP5** | PR #31 (세인 CLI design) cherry-pick — 컬러 + 배너만 추출 | PM | ✅ | #31 |

**최종 결과:**
- main: `142efad` (PR #34) → `29ad185` (README 갱신) → CLI cherry-pick 통합
- 단위 테스트 **227 통과 / 0 실패**
- valgrind 누수 0 (5 바이너리 모두)
- 옵션 B Mixed Merge 패턴 도입 (석제 PR #33 처리)
- Cherry-pick 부분 추출 패턴 도입 (세인 PR #31 처리, feature 브랜치 보존)

---

## 협업 패턴 정리

| 패턴 | 적용 PR | 핵심 |
|---|---|---|
| **Winner-Takes-All** | 1주차 INSERT (B vs C) | 두 PR 중 더 나은 쪽을 통째로 머지, 탈락 쪽엔 피드백 |
| **옵션 B Mixed Merge** | Phase 1 PR #32 vs #33 | 충돌 함수 단위로 비교 → 함수마다 베스트 코드 채택 |
| **Cherry-pick 부분 추출** | PR #31 (세인 CLI) | 작업자 브랜치 보존, 가치 있는 부분만 새 통합 커밋, `Co-Authored-By` 부착 |

공통 원칙:
- 작업자의 시간/노력은 항상 존중 (탈락해도 피드백, 미머지여도 cherry-pick + 크레딧)
- 머지 결정 기준: **테스트 + 메모리 안전 + 인터페이스 일관성**
- "우선권을 가지는 코드" 를 PM 이 명시 (예: Phase 1 storage.c 보호)

---

## 커밋 컨벤션 (Angular, 1주차와 동일)
```
feat:     새 기능 추가
fix:      버그 수정
test:     테스트 추가/수정
refactor: 리팩토링
docs:     문서, 주석
chore:    설정, 환경
```

예시:
- `feat(storage): RowSet 구조체 + storage_select_result 신설`
- `feat(parser): N-ary WHERE 지원 (3개 이상 조건)`
- `refactor(storage): UPDATE/DELETE 의 WHERE 평가 N-ary 로 확장`

---

## 머지 규칙 (1주차와 동일)
- feature → dev2 PR 은 본인이 직접 올린다
- 머지 승인은 지용 단독 (브랜치 보호로 강제됨)
- PR 올리기 전 아래 체크리스트 확인
- 모든 PR 은 GitHub Actions CI 자동 검증 + PM 코드 리뷰

### PR 체크리스트 (.github/pull_request_template.md 자동 적용)
- [ ] make 빌드 에러 / 경고 없음 (`-Wall -Wextra -Wpedantic`)
- [ ] 1주차 단위 테스트 회귀 0 (총 201, 호출 인자 갱신은 OK)
- [ ] 본인 영역 새 단위 테스트 추가
- [ ] valgrind 누수 0
- [ ] NULL / 파일 없을 때 / 빈 결과 에러 처리
- [ ] 인터페이스 계약 위반 0 (Phase 1 갱신된 시그니처 외 추가 변경 0)
- [ ] 커밋 컨벤션 준수

---

## 인터페이스 계약 (Phase 1 후 최종)

### 그대로 유지
```c
ParsedSQL *parse_sql(const char *input);
void       free_parsed(ParsedSQL *sql);
void       execute(ParsedSQL *sql);
int storage_insert(const char *table, char **columns, char **values, int count);
int storage_select(const char *table, ParsedSQL *sql);
int storage_create(const char *table, char **col_defs, int count);
```

### Phase 1 에서 시그니처 변경 (SELECT 와 통일)
```c
// 기존
int storage_delete(const char *table, WhereClause *where, int where_count);
int storage_update(const char *table, SetClause *set, int set_count,
                   WhereClause *where, int where_count);

// Phase 1 후
int storage_delete(const char *table, ParsedSQL *sql);
int storage_update(const char *table, ParsedSQL *sql);
```

`executor.c` 의 호출부도 같이 변경 (한 줄씩):
```c
case QUERY_DELETE: storage_delete(sql->table, sql); break;
case QUERY_UPDATE: storage_update(sql->table, sql); break;
```

### Phase 1 에서 신설 (한 번 머지 후 변경 금지)
```c
typedef struct {
    int     row_count;
    int     col_count;
    char  **col_names;     // ["id", "name", "age"]
    char ***rows;          // rows[i][j] = i 번째 행의 j 번째 컬럼 값
} RowSet;

int  storage_select_result(const char *table, ParsedSQL *sql, RowSet **out);
void print_rowset(FILE *out, const RowSet *rs);
void rowset_free(RowSet *rs);
```

### Phase 1 에서 ParsedSQL 확장 (N-ary WHERE 결합자)
```c
typedef struct {
    /* ... 기존 필드 ... */
    WhereClause *where;
    int          where_count;       // N 개 (1주차는 최대 2)
    char         where_logic[8];    // 1주차 호환 (deprecated, 모든 결합자 동일 시)
    char       **where_links;       // NEW: N-1 개의 결합자 ("AND" 또는 "OR" 문자열)
                                     //      길이 = where_count - 1
                                     //      where_count <= 1 이면 NULL
} ParsedSQL;
```

**구조는 MP1 PR 에서 확정. 그 전엔 사전 협의.**

### 집계 함수 — 인터페이스 변화 X
- 컬럼 이름 자리에 `"COUNT(*)"`, `"SUM(price)"`, `"AVG(age)"` 등 함수 호출형 문자열이 들어옴
- 1주차 PR #19 핫픽스로 이미 토큰 결합 동작
- storage 측에서 `is_aggregate_call(col_name, &fn, &arg)` 같은 검사 후 단일 행 RowSet 반환

---

## 협업 워크플로 (1주차와 동일)

```
git fetch origin
git checkout dev2
git pull origin dev2
git checkout -b feature/p1-<본인영역>

# 작업
make && make test                    # 회귀 0 확인
valgrind --leak-check=full -q ./test_runner

git add -A
git commit -m "feat(...): ..."
git push -u origin feature/p1-<본인영역>

# GitHub 에서 PR 생성 (base = dev2)
# CI green 확인
# PM 리뷰 후 머지
```

PR 들어오면 1주차와 동일한 PM 리뷰 워크플로:
1. CI green 확인
2. diff 분석 (인터페이스 계약 / 영역 침범 검사)
3. 로컬 체크아웃 → 빌드 / 테스트 / valgrind
4. 한국어 코드 리뷰 코멘트
5. 머지 결정

---

## 파일 구조 (현재 + Phase 1)

```
sql_parser/
├── include/
│   └── types.h              ← Phase 1: RowSet, 새 함수 선언, WhereClause 확장 (지용)
├── src/
│   ├── main.c               ← 1주차 그대로
│   ├── parser.c             ← Phase 1: stop set + N-ary WHERE (석제)
│   ├── ast_print.c          ← N-ary WHERE 표시 갱신 (석제)
│   ├── json_out.c           ← 동상 (석제)
│   ├── sql_format.c         ← 동상 (석제)
│   ├── executor.c           ← Phase 1: storage_delete/update 호출 인자 변경 (원우)
│   └── storage.c            ← Phase 1: RowSet 인프라 + 집계 함수 (지용)
│                              + UPDATE/DELETE 시그니처 통일 + 복합 WHERE 평가 (원우)
├── tests/
│   ├── test_parser.c        ← N-ary WHERE + 집계 컬럼 케이스 (석제)
│   ├── test_executor.c
│   ├── test_storage_select_result.c ← NEW: RowSet + 집계 함수 단위 테스트 (지용)
│   ├── test_storage_insert.c
│   ├── test_storage_delete.c ← 복합 WHERE 케이스 추가 + 호출 인자 갱신 (원우)
│   └── test_storage_update.c ← 동상 (원우)
├── data/                    ← (gitignored)
├── docs/
│   ├── QA_CHECKLIST.md
│   └── QA_REPORT_AUTO.md
├── server.py / index.html / query.sql / run_demo.sh
├── .github/workflows/build.yml + pull_request_template.md
├── Makefile
├── CLAUDE.md / agent.md (이 파일)
└── README.md
```

---

## FAQ — Phase 1

**Q. RowSet 만들면 server.py 도 바꿔야 하나요?**
A. 아니요. 1주차의 server.py 가 sqlparser 의 stdout 을 line-by-line 으로 파싱하는 hack 은 그대로 둡니다. RowSet 은 storage 내부와 향후 JOIN 등의 기반일 뿐, CLI 출력 동작은 그대로 유지됩니다. 외부 동작 변화 0 이 핵심 원칙.

**Q. N-ary WHERE 가 들어가면 1주차 테스트가 깨지지 않나요?**
A. 깨지면 안 됩니다. 1주차 테스트는 1~2 조건 케이스라 N-ary 의 부분집합. 회귀 0 이 PR 머지 조건. UPDATE/DELETE 의 시그니처 변경 때문에 호출 인자 갱신은 필요 (테스트 본문은 거의 동일).

**Q. UPDATE/DELETE 의 WHERE 가 SELECT 의 WHERE 와 같은 평가 함수를 쓰나요?**
A. Phase 1 후 **같은 평가 함수** 로 통일하는 게 권장. 원우가 `evaluate_where_with_links()` 같은 공용 헬퍼를 storage.c 에 만들고, SELECT/DELETE/UPDATE 모두 사용. 지용 RowSet 작업과도 자연스럽게 통합 가능.

**Q. 집계 함수 GROUP BY 도 지원하나요?**
A. **GROUP BY 는 Phase 2 이후.** Phase 1 의 집계 함수는 전체 행에 대한 단일 집계만. `SELECT SUM(price) FROM orders` 처럼 한 줄짜리 결과만 지원. `SELECT dept, SUM(salary) FROM emp GROUP BY dept` 는 미지원.

**Q. SUM/AVG 의 타입 제한은?**
A. INT, FLOAT 만. VARCHAR/BOOLEAN/DATE/DATETIME 에 SUM/AVG 시도하면 stderr 에 친절한 에러. MIN/MAX 는 모든 타입 (DATE 도 문자열 비교로 정렬 가능).

**Q. dev2 가 아니라 dev 로 가도 되나요?**
A. 안 됩니다. 1주차 dev / main 은 발표용 안정 상태로 보존. Phase 1 작업은 전부 dev2 에서.

**Q. storage_delete/update 시그니처 변경하면 1주차 호환성 깨지지 않나요?**
A. 깨집니다. **의도된 변경**입니다. Phase 1 의 핵심이 인터페이스 정리. 1주차 main 은 그대로 보존되고 (안정 발표 상태), Phase 1 의 새 시그니처는 dev2 에서 시작해서 main 으로 머지됩니다. 1주차 시연이 필요하면 main 의 1주차 commit 을 checkout.

**Q. 막히면?**
A. M3 (1차 구현) 까지 못 가면 즉시 지용에게 알리세요. 혼자 1시간 이상 붙잡지 말 것. 1주차 의 세인 케이스 같은 작업 손실 방지가 PM 의 최우선 책임입니다.
