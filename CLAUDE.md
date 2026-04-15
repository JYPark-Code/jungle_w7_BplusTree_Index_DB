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

---

## 그라운드 룰

1. `include/types.h`, `include/bptree.h` **절대 수정 금지** — 인터페이스 계약 파일
2. 커밋은 **Angular Commit Convention** 준수
3. 기능 완성 후 **AI에게 unit test 작성 위임** → 테스트 통과 확인 후 PR
4. 병렬 작업 가능하도록 담당 파일 외 수정 금지
5. MP1(bptree.h 확정) 머지 전까지 본인 작업 시작 X
6. 막히면 1시간 이내 지용에게 알릴 것

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
