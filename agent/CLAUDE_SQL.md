# Claude.md — 지용님 PM 컨텍스트

## 프로젝트 요약
- 수요코딩회 1주차 1일 프로젝트 (완료) → **2주차 phase 1 리팩토링**
- C로 파일 기반 SQL 처리기 + Python 중계 서버 + HTML 뷰어
- 1주차 팀: 지용(PM+Parser), 석제(SELECT), 원우(INSERT/DELETE/UPDATE)
- 2주차 phase 1 팀: 지용 + 석제 + 원우 (3명, dev2 브랜치)

---

## 1주차 완료 상태 (참고)

| 영역 | 결과 |
|---|---|
| Parser (5종 쿼리, COUNT(*), LIKE, ORDER BY, LIMIT) | ✅ 지용 |
| Storage SELECT/CREATE | ✅ 석제 |
| Storage INSERT/DELETE/UPDATE | ✅ 원우 |
| CLI 6 종 플래그 (--debug/--json/--tokens/--format/--help/--version) | ✅ |
| server.py + index.html (CodeMirror, Cards/JSON 토글, inline 결과 표) | ✅ |
| GitHub Actions CI / 201 단위 테스트 / valgrind 0 | ✅ |
| main 머지 | ✅ |

---

# 🔄 2주차 Phase 1 — 인터페이스 리팩토링 (subquery 안 함)

## 목표
1주차의 가장 큰 약점 해소: **storage 가 결과를 stdout 으로만 print 하고 데이터로 반환 못 함**.
이걸 풀어주면 2주차의 B+트리/JOIN/집계/subquery 모든 길이 열린다.

## Phase 1 작업 5가지 (3명 분업)

### A. RowSet 인프라 + 집계 함수 (지용) ⭐ 핵심
- `include/types.h` 에 `RowSet` 구조체 정의 (행/컬럼 메모리 표현)
- `storage_select_result(table, sql, RowSet **out)` 신설
- `print_rowset(FILE*, RowSet*)` helper
- `rowset_free(RowSet*)`
- 기존 `storage_select` 는 **얇은 wrapper** 로 리팩토링
  (`storage_select_result` 호출 → `print_rowset` → `rowset_free`)
- **집계 함수 평가**: SUM(col), AVG(col), MIN(col), MAX(col)
  - COUNT(*) 는 1주차에 이미 동작
  - 같은 메커니즘 (함수 호출형 컬럼 인식) 을 SUM/AVG/MIN/MAX 에 확장
  - 결과는 단일 행 RowSet 으로 반환
- 단위 테스트: RowSet 직접 검증 + 집계 함수 케이스
- **외부 동작 변화 0** — 모든 기존 테스트 통과해야 함

### B. Parser stop set + N-ary WHERE (석제)
- `parse_select` 에 stop set 도입 (`)`, `;` 같은 곳에서 중단)
  → 향후 subquery / 괄호 그룹화 대비
- `parse_where` 를 N-ary 로 확장 (현재는 1~2 조건만)
  → `WHERE a=1 AND b=2 AND c=3` 같이 N개 조건 지원
- `WhereClause` 배열을 N개로 동적 확장
- 결합자 배열 추가 (조건 사이마다 AND/OR)
- 집계 함수 컬럼 (`SUM(col)`, `AVG(col)` 등) 을 컬럼 자리에서 받기
  → COUNT(*) 와 같은 함수 호출형 인식 메커니즘 그대로 (이미 동작, 회귀 검증만)
- 단위 테스트: 3개 이상 조건, 혼합 결합, 집계 함수 컬럼 파싱 케이스

### C. UPDATE/DELETE 시그니처 통일 + 복합 WHERE (원우)
- **시그니처 변경 (인터페이스 계약 갱신)**:
  ```c
  // 기존 (1주차)
  int storage_delete(const char *table, WhereClause *where, int where_count);
  int storage_update(const char *table, SetClause *set, int set_count,
                     WhereClause *where, int where_count);

  // Phase 1 후 — SELECT 와 동일 패턴 (ParsedSQL* 한 인자)
  int storage_delete(const char *table, ParsedSQL *sql);
  int storage_update(const char *table, ParsedSQL *sql);
  ```
- 호출부 (`executor.c`) 도 같이 변경 — `storage_delete(sql->table, sql)` / `storage_update(sql->table, sql)`
- N-ary WHERE 가 정상 평가 (석제의 결합자 배열 사용)
- UPDATE 의 SET 도 ParsedSQL 안에서 그대로 접근
- 단위 테스트: 복합 WHERE 케이스 추가 (3개 이상 조건)
- 호출부 변경: `executor.c` 의 case 본문도 같이 갱신

### D. 인터페이스 계약 정리 (지용, MP1 먼저) ⭐ 첫 PR
- `include/types.h` 에 한 번에:
  - `RowSet` 구조체 (지용 RowSet 작업 prerequisite)
  - `ParsedSQL` 의 N-ary 결합자 (`where_links` 배열, 석제 작업 prerequisite)
  - `storage_delete` / `storage_update` 시그니처 변경 (원우 작업 prerequisite)
  - 새 함수 선언: `storage_select_result`, `rowset_free`, `print_rowset`
- 인터페이스 계약 PR 머지 전엔 셋 다 작업 시작 X
- 한 번 머지된 후엔 시그니처 추가는 가능, 변경은 협의 필수

### E. 집계 함수 — 분담
- **파서 측** (석제): 1주차 COUNT(*) 핫픽스 (PR #19) 의 메커니즘이 `SUM(col)` `AVG(col)` 등에도 그대로 적용됨. 회귀 검증만.
- **실행 측** (지용): storage_select 에서 컬럼이 함수 호출형 (`SUM(price)` 등) 이면 그 컬럼에 대해 집계 계산. COUNT(*) 처리 (`is_count_star`, `print_selection` 의 분기) 를 SUM/AVG/MIN/MAX 로 확장.
- 단위 테스트는 양쪽 다.

---

## 분업 의존성

```
[D. 인터페이스 계약 정리 — 지용]
   • RowSet
   • N-ary WHERE 결합자
   • storage_delete/update 시그니처 변경
              ↓ (먼저 머지)
   ┌──────────┼──────────────────────┐
   ↓          ↓                      ↓
[A. RowSet  [B. Parser stop set    [C. UPDATE/DELETE
   + 집계      + N-ary WHERE          시그니처 통일
   실행]       + 집계 컬럼 회귀]      + N-ary WHERE 평가]
 (지용)       (석제)                  (원우)
   └──────────┬──────────────────────┘
              ↓
        [통합 + 회귀 검증]
              ↓
        [dev2 → main]
```

---

## 지용님 담당
1. **인터페이스 계약 PR 먼저 머지** (D) — `feature/p1-interface-contract`
2. **RowSet 인프라 + 집계 함수 실행** (A + E) — `feature/p1-rowset`
3. **PM 역할**: 석제/원우 PR 리뷰, 머지 결정, 통합 책임
4. CI / PR 템플릿 / 머지 워크플로 1주차와 동일 운영

---

## 머지 포인트 (Phase 1) — ✅ 모두 완료

### MP1 — 인터페이스 계약 (지용) — PR #29 ✅
- [x] `types.h` 에 RowSet 구조체, `where_links` 필드, 새 함수 선언
- [x] dev2 에 머지
- [x] 석제/원우에게 alert + 본인 브랜치 rebase

### MP2 — 각자 PR 머지 ✅
- [x] 지용 — RowSet 인프라 + 집계 함수 5종 (`feature/p1-rowset`) — **PR #30**
- [x] 원우 — UPDATE/DELETE 시그니처 통일 + N-ary WHERE 평가 (`feature/p1-compound-where`) — **PR #32**
- [x] 석제 — Parser stop set + N-ary WHERE + 출력 모듈 갱신 (`feature/p1-parser-stop-set`) — **PR #33**
  - 옵션 B Mixed Merge 로 처리 (원우 PR 과 함수 단위 비교 후 베스트 통합)

각 PR 1주차와 동일 워크플로 적용:
- ✅ CI green
- ✅ PM 코드 리뷰
- ✅ valgrind 누수 0 (5 바이너리 모두)
- ✅ 단위 테스트 추가

### MP3 — Phase 1 통합 ✅
- [x] 세 PR 모두 머지된 dev2 에서 빌드 무경고
- [x] 1주차 단위 테스트 회귀 0 (201 → 197 + 30 신규 = 227 통과)
- [x] 새 단위 테스트 (RowSet 30 + N-ary WHERE) 통과
- [x] valgrind 누수 0
- [x] CLI / 브라우저 뷰어 동작 동일 확인

### MP4 — dev2 → main 머지 ✅ (PR #34)
- [x] dev2 → main PR
- [x] admin 머지 — main `142efad`
- [x] README 1차 → 후속 흐름 갱신 (`29ad185`)

### MP5 — PR #31 (세인 CLI design) Cherry-pick ✅
- [x] feature/CLI-design 브랜치는 건들지 않음 (작업자 권리 보존)
- [x] 옵션 1: cherry-pick 으로 **컬러 + 배너만** 추출 (Phase 1 storage.c 100% 보존)
- [x] `ast_print.c` ANSI 컬러 + `isatty()` 가드 (메모리 스트림 fixture 안전)
- [x] `storage.c` 테이블 헤더 컬러 + `storage_file_is_tty()` 가드
- [x] `main.c` `print_welcome_banner()` (Hello Kitty + WELCOME + SQL PARSER)
- [x] 한국어 비교 평가 + 통합 커밋 `Co-Authored-By` 부착

---

## 협업 패턴 정리 (1주차 → Phase 1 → CLI cherry-pick)

| 패턴 | 적용 PR | 핵심 |
|---|---|---|
| **Winner-Takes-All** | 1주차 INSERT (B vs C) | 두 PR 중 더 나은 쪽을 통째로 머지, 탈락 쪽엔 피드백 |
| **옵션 B Mixed Merge** | Phase 1 PR #32 vs #33 | 충돌 함수 단위로 비교 → 함수마다 베스트 코드 채택 |
| **Cherry-pick 부분 추출** | PR #31 (세인 CLI design) | 작업자 브랜치 보존, 가치 있는 부분(컬러+배너)만 골라 새 통합 커밋, Co-Authored-By 부착 |

세 패턴 공통 원칙:
- 작업자의 시간/노력은 항상 존중 (탈락해도 피드백, 미머지여도 cherry-pick + 크레딧)
- 머지 결정의 기준은 **테스트 + 메모리 안전 + 인터페이스 일관성**
- "우선권을 가지는 코드" 를 PM 이 명시 (예: Phase 1 storage.c 보호)

---

## storage 인터페이스 계약 (Phase 1 갱신)

### Phase 1 후 최종 형태

```c
/* 기존 — 시그니처 그대로 유지 */
int storage_insert(const char *table, char **columns, char **values, int count);
int storage_select(const char *table, ParsedSQL *sql);
int storage_create(const char *table, char **col_defs, int count);

/* Phase 1 에서 시그니처 변경 — SELECT 와 동일 패턴 (ParsedSQL* 한 인자) */
int storage_delete(const char *table, ParsedSQL *sql);
int storage_update(const char *table, ParsedSQL *sql);

/* Phase 1 에서 신설 */
int  storage_select_result(const char *table, ParsedSQL *sql, RowSet **out);
void print_rowset(FILE *out, const RowSet *rs);
void rowset_free(RowSet *rs);
```

### 변경 사유
- `storage_delete` / `storage_update` 가 N-ary WHERE 와 결합자 배열 (`where_links`) 에 접근하려면 `WhereClause *` + `int` 두 인자 만으로는 부족
- SELECT 와 동일하게 `ParsedSQL *` 한 인자로 통일 → API 일관성 + 내부 코드 간결
- 호출부 `executor.c` 도 같이 변경 (한 줄씩, 단순)

### 원칙
- 1주차에 고정한 인터페이스 계약은 **MP1 PR 한 번** 으로 갱신 후 다시 고정
- MP1 머지 전: 셋 다 작업 시작 X
- MP1 머지 후: 셋이 동시 작업 가능, 시그니처 추가는 협의 후 OK

---

## 위기 대응 (Phase 1)

| 상황 | 대응 |
|------|------|
| 인터페이스 계약 충돌 | MP1 의 지용 PR 을 다시 받아서 정리 |
| RowSet 메모리 누수 | valgrind 로 즉시 잡기, free 경로 재검증 |
| N-ary WHERE 회귀 | 1주차 테스트가 fail 나면 즉시 롤백 후 재설계 |
| 셋 중 한 명 막힘 | 페어링 / vibe coding 지원 / 범위 축소 |
| 통합 후 빌드 깨짐 | dev2 에서 hotfix 브랜치, 1주차와 동일 |

---

## 위기 대응 (1주차 회상 — 참고)
| 상황 | 1주차 대응 |
|------|------|
| 팀원 M2 기준 미달 | 즉시 vibe coding으로 지원 |
| 통합 후 빌드 에러 | main.c에서 미완성 모듈 주석 처리 후 진행 |
| HTML UI 시간 부족 | 미련 없이 CLI만으로 발표 |
| B/C 둘 다 미완성 | 완성된 부분만 머지, INSERT 기본만 시연 |

---

## 2주차 phase 2 이후 (Phase 1 끝나면)
- B+트리 도입 (storage 내부 구현 교체)
- 해시 인덱스
- subquery (스칼라 → IN → FROM 순)
- JOIN (inner / left)
- 집계 함수 (SUM/AVG/MIN/MAX/GROUP BY)
- DATETIME 타입 실제 비교
- 트랜잭션/로그 (시간 남으면)

Phase 1 의 RowSet 인프라가 위 모든 것의 기반.
