# CLAUDE.md — MiniSQL Week 7: B+ Tree Index

## 프로젝트 개요

Week 6 SQL 처리기에 B+ 트리 인덱스를 추가하는 1일 스프린트.  
구현 언어: C. 인터페이스: CLI. 저장: CSV 파일 기반.

---

## 팀 구성 및 역할

| 이름 | 역할 | 브랜치 |
|---|---|---|
| 지용 (PM) | `bptree.c` 코어 + 인터페이스 확정 + 레포/Makefile + 머지 | `feature/bptree-core` |
| 정환 | `executor.c` WHERE id 분기 + SQL 처리기 이식 검증 | `feature/executor-index` |
| 민철 | `storage.c` auto-increment id + `bptree_insert` 연동 | `feature/storage-autoid` |
| 규태 | `bench/benchmark.c` + 더미 데이터 생성기 + README | `feature/benchmark` |
| 규태 (추가) | `web/` 웹 데모 (정적 HTML + Python stdlib 중개 서버) | `feature/web-demo` |

---

## 그라운드 룰

1. `include/bptree.h` **절대 수정 금지** — 인터페이스 계약 파일
   - `include/types.h` 는 **PM(지용) 단독 PR로만** 수정 (팀원은 건드리지 않음)
2. 커밋은 **Angular Commit Convention** 준수
3. 기능 완성 후 **AI에게 unit test 작성 위임** → 테스트 통과 확인 후 PR
4. 담당 파일 외 수정은 지양하되, **성능/기능상 불가피하면 허용** — 같은 파일에 두 팀원 PR이 오면 **PM이 Mix merge로 정리**
   - 특히 `src/storage.c`, `src/executor.c` 는 executor/storage 양측이 공유하는 핫패스라 공동 편집 허용
5. 선행 블로커(types.h/파서 확장) PR 머지 전까지 의존 작업 시작 X
6. 막히면 1시간 이내 지용에게 알릴 것

---

## Round 2 (2026-04-15~) — BETWEEN 실행경로 + DELETE/UPDATE 인덱스 동기화

Round 1 (MP1~MP4) 는 main 으로 배포 완료. Round 2 는 "발표 임팩트 + 안정성" 중심.

### Phase 1 — 선행 블로커 (지용 단독 PR)
- `include/types.h`: `WhereClause` 에 `value_to[256]` 추가 (BETWEEN 상한 값)
- `include/types.h`: `storage_select_result_by_row_indices(table, sql, int* ids, int n, RowSet**)` 선언 추가
- `src/parser.c`: `BETWEEN A AND B` 파싱 → `op="BETWEEN"`, `value=A`, `value_to=B` normalize
- `tests/test_parser.c`: BETWEEN 파싱 테스트 1~2건

### Phase 2 — 병렬 작업 (Mix merge 전제)

| 담당 | 작업 | 건드리는 파일 | 브랜치 |
|---|---|---|---|
| **정환** | BETWEEN 실행 경로 (`executor_try_range_select` → `bptree_range` → `storage_select_result_by_row_indices`) + 테스트 | `src/executor.c`, `src/storage.c`(구현체), `tests/test_executor.c` | `feature/executor-between` |
| **민철** | DELETE/UPDATE 시 인덱스 동기화 (현 라운드는 **인덱스 rebuild 방식**으로 안전하게, `bptree_delete` 신설은 보너스) + 테스트 | `src/storage.c`, `tests/test_storage_*.c` | `feature/storage-index-sync` |
| **지용** | (1) Phase 1 선행, (2) 선형 vs 인덱스 **비교** 벤치 추가, (3) Mix merge | `bench/benchmark.c`, `README.md`, PM 영역 | `chore/round2-*` |
| **규태** | MP5 웹 데모 진행 중 (병행, 본진 영향 없음) | `web/` | `feature/web-demo` |

### Phase 3 — Mix merge + main 배포
- 정환 PR + 민철 PR 받으면 지용이 Mix merge PR → dev
- MP5 웹 데모 머지 후 dev → main 최종

### Round 2 머지 포인트 (MP6~)
| MP | 시점 | 조건 |
|---|---|---|
| **MP6** | Phase 1 | `include/types.h` 확장 + parser BETWEEN + 테스트 통과 머지 |
| **MP7** | Phase 2 정환 PR | BETWEEN SELECT E2E 동작 + 테스트 통과 |
| **MP8** | Phase 2 민철 PR | DELETE/UPDATE 후 `WHERE id=?` 조회 정합성 유지 |
| **MP9** | Phase 3 | Mix merge + 선형 vs 인덱스 비교 벤치 수치 README 반영 |
| **MP10** | 규태 MP5 | 웹 데모 완성 → dev → main

---

## 커밋 컨벤션 (Angular)

```
feat:     새 기능 추가
fix:      버그 수정
test:     테스트 추가/수정
refactor: 리팩토링
docs:     문서, 주석
chore:    설정, 환경
```

예시:
```
feat(bptree): leaf split 구현
feat(storage): auto-increment id + bptree_insert 연동
feat(executor): WHERE id=? 조건 시 bptree_search 분기
feat(bench): 100만 건 INSERT + SELECT 성능 측정 스크립트
test(bptree): leaf split 단위 테스트 추가
```

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
# 브랜치 시작 (MP1 머지 후)
git fetch origin
git checkout dev
git pull origin dev
git checkout -b feature/<본인영역>
```

---

## 인터페이스 계약 (수정 금지)

### bptree.h (신설, MP1에서 확정)

```c
typedef struct BPTree BPTree;

BPTree *bptree_create(int order);
void    bptree_insert(BPTree *tree, int id, int row_index);
int     bptree_search(BPTree *tree, int id);   // row_index 반환, 없으면 -1
int     bptree_range(BPTree *tree, int from, int to, int *out, int max_out);
void    bptree_print(BPTree *tree);
void    bptree_destroy(BPTree *tree);
```

### storage.c 변경 (민철)

```c
// 기존 그대로 — 시그니처 변경 없음
int storage_insert(const char *table, char **columns, char **values, int count);
// 내부에서 next_id++ 후 bptree_insert(tree, id, row_idx) 호출만 추가
```

### executor.c 변경 (정환)

```c
// WHERE id=? 감지 분기만 추가 — 기존 선형 탐색 유지
if (strcmp(where_col, "id") == 0) {
    int row_idx = bptree_search(tree, atoi(where_val));
    // row_idx로 직접 접근
} else {
    // 기존 선형 탐색 그대로
}
```

---

## 타임라인

| 시간 | 지용 | 정환 | 민철 | 규태 |
|---|---|---|---|---|
| 10:30–11:00 | 레포 세팅 + bptree.h 확정 → **MP1** | 환경 세팅 | 환경 세팅 | 환경 세팅 |
| 11:00–12:00 | bptree.c 구조체 + search | executor.c Week6 이식 확인 + 분기 설계 | storage.c auto-id 구현 | benchmark.c 더미 생성기 착수 |
| 12:00–13:00 | 🍽 점심 | 🍽 점심 | 🍽 점심 | 🍽 점심 |
| 13:00–14:30 | bptree_insert (split 없이) → **MP2** | WHERE id 분기 구현 | bptree_insert 연동 | benchmark INSERT 루프 |
| 14:30–16:00 | leaf split | 단위 테스트 + PR | 단위 테스트 + PR | SELECT 성능 측정 |
| 16:00–17:30 | internal/root split → **MP3** | 통합 테스트 지원 | 통합 테스트 지원 | 결과 출력 + 단위 테스트 + PR |
| 17:30–18:00 | 전체 통합 빌드 + 회귀 테스트 | — | — | — |
| 18:00–19:00 | 🍽 저녁 | 🍽 저녁 | 🍽 저녁 | 🍽 저녁 |
| 19:00–20:30 | valgrind + 버그 수정 → **MP4** | 버그 수정 지원 | 버그 수정 지원 | README 업데이트 |
| 20:30–21:00 | dev → main 머지 + 발표 준비 | 리허설 참여 | 리허설 참여 | 리허설 참여 |
| (예비) 21:00– | 2차 리팩토링 or 추가 기능 | — | — | — |

---

## 머지 포인트

| MP | 시점 | 조건 | 담당 |
|---|---|---|---|
| **MP1** | ~11:00 | `bptree.h` 확정 + 레포 세팅 완료 | 지용 |
| **MP2** | ~13:00 | `bptree_search` + `bptree_insert` (split 없이) 동작 | 지용 |
| **MP3** | ~17:30 | split 완성 + 정환/민철 PR 머지 + 통합 빌드 통과 | 지용 |
| **MP4** | ~20:30 | 100만 건 테스트 + valgrind 0 + 규태 PR 머지 | 지용 |
| **최종** | ~21:00 | dev → main 머지 | 지용 |

---

## PR 체크리스트

PR 올리기 전 반드시 확인:

- [ ] `make` 빌드 경고 없음 (`-Wall -Wextra -Wpedantic`)
- [ ] Week 6 단위 테스트 회귀 0
- [ ] 본인 영역 새 단위 테스트 추가 (AI 생성 OK, 통과 여부 확인 필수)
- [ ] `valgrind --leak-check=full` 누수 0
- [ ] `bptree.h`, `types.h` 변경 없음
- [ ] 담당 파일 외 변경 없음
- [ ] Angular 커밋 컨벤션 준수

---

## 위기 대응

| 상황 | 대응 |
|---|---|
| MP2 (13:00)까지 bptree_insert 미완성 | split 없이 insert만 있어도 연동 먼저 진행, split은 후속 |
| 정환/민철 PR이 MP3 전 미완성 | 완성된 부분만 stub으로 머지 후 진행 |
| split 로직 버그로 MP3 지연 | 지용이 valgrind 없이 먼저 머지, 저녁 후 수정 |
| 규태 벤치마크 미완성 | CLI 수동 측정 결과로 대체 |
| 발표 전 빌드 에러 | 문제 모듈 주석 처리 후 진행 |

---

## 파일 구조

```
.
├── include/
│   ├── types.h          ← Week 6 그대로 (수정 금지)
│   └── bptree.h         ← NEW (수정 금지, MP1 확정 후)
├── src/
│   ├── main.c
│   ├── parser.c         ← 수정 없음
│   ├── ast_print.c      ← 수정 없음
│   ├── json_out.c       ← 수정 없음
│   ├── sql_format.c     ← 수정 없음
│   ├── executor.c       ← 정환
│   ├── storage.c        ← 민철
│   └── bptree.c         ← 지용
├── tests/
│   ├── test_parser.c
│   ├── test_executor.c
│   ├── test_storage_*.c
│   └── test_bptree.c    ← 지용 (AI 생성)
├── bench/
│   └── benchmark.c      ← 규태
├── web/                 ← 규태 (보너스: 발표 시연용 웹 UI)
│   ├── index.html       ← SQL 입력창 + 결과 테이블 + Chart.js 벤치 차트
│   ├── app.js           ← fetch 로 /api/query, /api/bench 호출
│   └── server.py        ← http.server 기반 중개 (stdlib만, 의존성 0)
├── Makefile
├── CLAUDE.md            ← 이 파일
├── agent.md
└── README.md
```

---

## 빌드 명령어

```bash
make              # 전체 빌드
make test         # 단위 테스트
make bench        # 벤치마크
make valgrind     # 누수 검사
make clean
```

---

## 웹 데모 (규태, 보너스 과제)

발표 시연 임팩트 강화를 위한 선택 과제. **본진 MP4 머지 완료 후**에만 착수.

### 스택 (의존성 0 원칙)
- 프론트: 정적 HTML + vanilla JS + Chart.js CDN 한 줄
- 백엔드: `python3 -m http.server` 계열 — `http.server.BaseHTTPRequestHandler` 직접 상속, stdlib만
- 빌드 파이프라인 없음 (npm / node / webpack X)

### 동작 방식
1. `server.py` 가 `./sqlparser`, `./benchmark` 를 `subprocess` 로 호출
2. `POST /api/query` → sqlparser `--json` 모드 실행 → 결과 JSON 반환
3. `POST /api/bench` → benchmark 실행 → stdout 파싱해 차트용 JSON 반환
4. 정적 파일(`index.html`, `app.js`)도 같은 포트에서 서빙 → CORS 불필요

### 제약
- `include/bptree.h`, `include/types.h`, 기존 C 소스 **수정 금지** — 웹 레이어에서만 작업
- 기존 `make test` / `make bench` 회귀 0 유지
- `Makefile` 수정 필요 시 지용에게 PR 요청

### 시연 시나리오 — 결제/트랜잭션 로그 (발표 메인)

**발표용 핵심 멘트:**
> **"장애 발생 시 특정 시간 구간의 트랜잭션 로그를 빠르게 조회해야 한다 — B+Tree range query 가 O(log n + k) 로 해결한다."**

**데이터 모델 (브라우저에서 시연용으로 실행):**
```sql
CREATE TABLE payments (
    id         INT,         -- auto-increment, 시간순 proxy
    user_id    INT,
    amount     INT,
    status     TEXT,         -- 'SUCCESS' | 'FAIL' | 'TIMEOUT'
    created_at INT           -- Unix timestamp
);
```

**시연 3단 구성 (~3분):**
1. **더미 주입 (5초)** — 10만~100만 건 결제 로그, 실패율 5% / 타임아웃 2% 섞어서 생성
2. **장애 구간 조회 (30초)** — "새벽 3시 장애 대응" 상황극 → `SELECT * FROM payments WHERE id BETWEEN 30000 AND 31500` → 1ms 내 반환 표시
3. **선형 vs 인덱스 비교 차트 (1분)** — 같은 범위를 선형 탐색(name 조건)으로 돌려서 막대그래프 2개, "400배 단축" 강조

**UI 버튼 3개 (최소):**
- [ 더미 주입 ] [ 장애 구간 조회 (range) ] [ 선형 vs 인덱스 비교 ]

### 머지 조건 (MP5 — 선택)
- [ ] `python3 web/server.py` 로 로컬 실행 확인
- [ ] 브라우저에서 **결제 로그 더미 주입 / range 조회 / 비교 차트** 3 시나리오 동작
- [ ] `bptree_range` 호출 결과가 1ms 내 반환됨을 UI 에 명시
- [ ] README 에 실행 방법 1줄 추가

---

## FE 와 본진 팀원 영향도 (동시 개발 시 주의사항)

**원칙:** FE 는 `./sqlparser --json` 바이너리의 stdout 만 읽는다. C API 직접 호출 없음.

| 팀원 | 파일 | 충돌 위험 | 주의사항 |
|---|---|---|---|
| 지용 | `bptree.c`, `bptree.h`, `Makefile` | 🟢 없음 | FE 는 C API 직접 사용 X, 인터페이스 변화 무관 |
| 정환 | `executor.c`, `json_out.c` | 🟡 **JSON 스키마 합의 필요** | `--json` 출력 필드명/구조 변경 시 FE 파싱 깨짐 → PR 본문에 스키마 명시 |
| 민철 | `storage.c` | 🟢 거의 없음 | auto-increment id 를 stdout/JSON 에 포함하는지만 FE 에 공유 |
| 규태 | `bench/`, `web/` | 🟢 본인 영역 | `benchmark.c` stdout 포맷 바꾸면 `server.py` 파서 같이 수정 |

**JSON 스키마 계약 (MP5 착수 시점 기준 동결):**
- FE 착수 시 현재 `./sqlparser --json` 출력 스키마를 `web/README.md` 에 박아두기
- 이후 스키마 변경은 정환/규태 합의 후에만 (FE 에서 5분 내 대응 가능한 범위)

**동시 진행 권장 순서:**
1. 규태: 현재 `--json` 스펙 고정 → FE 착수
2. 정환/민철 PR 머지 후 → FE 가 출력 diff 확인 → 필요시 파서만 수정
3. 지용: 본진만 집중, FE 리뷰는 MP5 시점에만
