# CLAUDE.md — 지용 전용

## 내 역할 요약

PM + `bptree.c` 코어 구현 + 인터페이스 설계 + 머지 담당.  
팀에서 가장 어려운 split 로직을 맡고, 전체 통합을 책임진다.

---

## 작업 파일

| 파일 | 내용 |
|---|---|
| `include/bptree.h` | MP1에서 확정, 이후 수정 금지 |
| `src/bptree.c` | B+ 트리 전체 구현 |
| `Makefile` | bptree.c + bench 타겟 추가 |
| `tests/test_bptree.c` | 단위 테스트 (AI 생성) |

---

## 내 작업 순서 (함수 단위 커밋)

### Phase 1 — 인터페이스 + 골격 (10:30~11:00)

```bash
# 1. 레포 세팅
git checkout -b feature/bptree-core

# 2. bptree.h 작성 후 팀 공유
# 3. Makefile에 bptree.c 추가
# 4. MP1 PR → dev 머지
```

커밋:
```
chore(repo): 레포 세팅 + devcontainer 확인
feat(bptree): bptree.h 인터페이스 확정
chore(makefile): bptree.c + bench 타겟 추가
```

---

### Phase 2 — 구조체 + search (11:00~12:00)

구현 순서:
1. `BPTree`, `LeafNode`, `InternalNode` 구조체
2. `bptree_create()` / `bptree_destroy()`
3. `bptree_search()` — 루트에서 리프까지 재귀

각 함수 완성 시 즉시 커밋 + 푸시:
```
feat(bptree): BPTree/LeafNode/InternalNode 구조체 정의
feat(bptree): bptree_create / bptree_destroy 구현
feat(bptree): bptree_search 구현 (루트→리프 탐색)
```

---

### Phase 3 — insert + MP2 (13:00~14:30)

1. `bptree_insert()` — overflow 없는 단순 삽입
2. 10건 내외로 직접 테스트
3. **MP2 PR → dev 머지** (정환/민철이 연동 시작 가능)

```
feat(bptree): bptree_insert 구현 (split 없는 버전)
test(bptree): 단순 삽입 + search 검증 (AI 생성)
```

---

### Phase 4 — leaf split (14:30~16:00)

leaf split이 핵심. 포인터 순서 꼭 지킬 것:
1. 새 리프 노드 할당
2. 키 절반을 새 노드로 복사
3. `next` 포인터 연결 (linked list 유지)
4. 중간 키를 부모로 올림

```
feat(bptree): leaf split 구현
test(bptree): leaf split 후 search 정합성 검증 (AI 생성)
```

---

### Phase 5 — internal/root split + MP3 (16:00~17:30)

leaf split 패턴 그대로 반복. root split만 예외:
- 새 루트 할당 → 기존 루트가 자식이 됨

```
feat(bptree): internal node split 구현
feat(bptree): root split 구현 (트리 높이 증가)
test(bptree): 대량 삽입 후 전수 search 검증 (AI 생성)
```

MP3 조건: split 완성 + 정환/민철 PR 머지 + 통합 빌드 227+ 통과

---

### Phase 6 — valgrind + 통합 (19:00~20:30)

```bash
valgrind --leak-check=full --error-exitcode=1 ./sqlparser
valgrind --leak-check=full --error-exitcode=1 ./test_runner
valgrind --leak-check=full --error-exitcode=1 ./bench/benchmark
```

누수 발견 시 `bptree_destroy()` 먼저 확인.

```
fix(bptree): bptree_destroy 메모리 해제 누락 수정
```

---

## 커밋 규칙 (내 기준)

- **함수 1개 완성 = 커밋 1개 + 즉시 푸시**
- 테스트 통과 확인 후 커밋
- 커밋 메시지: `feat(bptree): 함수명 구현` 형식

```
feat(bptree): bptree_search 구현
test(bptree): bptree_search 단위 테스트 (AI 생성)
feat(bptree): bptree_insert 구현 (split 없는 버전)
feat(bptree): leaf split 구현
test(bptree): leaf split 후 정합성 검증 (AI 생성)
feat(bptree): internal/root split 구현
fix(bptree): split 후 부모 키 업데이트 버그 수정
```

---

## 단위 테스트 (AI 위임 방식)

각 Phase 완료 후 Claude Code에 아래 프롬프트 사용:

```
다음 함수에 대한 C 단위 테스트를 tests/test_bptree.c에 작성해줘.
테스트 프레임워크는 기존 test_runner 패턴 그대로.

[구현한 함수 + 구조체 붙여넣기]

테스트 케이스:
1. 정상 케이스
2. 경계값 (빈 트리, 단일 원소)
3. 대량 삽입 후 전수 검색
4. NULL/에러 케이스
```

---

## PM 체크리스트

### MP1 (11:00)
- [ ] `bptree.h` 팀 전체 공유 완료
- [ ] `g_tree` 전역 선언 위치 합의 (`main.c`)
- [ ] 팀원 전원 브랜치 생성 확인

### MP2 (13:00)
- [ ] `bptree_search` + `bptree_insert` (split 없이) 동작
- [ ] 정환/민철에게 stub → 실함수 교체 공지
- [ ] feature/bptree-core → dev PR 머지

### MP3 (17:30)
- [ ] 정환 PR 리뷰 + 머지
- [ ] 민철 PR 리뷰 + 머지
- [ ] 통합 빌드 `make test` 227+ 통과

### MP4 (20:30)
- [ ] 규태 PR 리뷰 + 머지
- [ ] 100만 건 테스트 통과
- [ ] 5개 바이너리 valgrind 누수 0

### 최종 (21:00) — **Round 1 완료 (2026-04-15)**
- [x] dev → main PR + 머지 (PR #18)
- [x] README 최종 수치 업데이트 (PR #17)

### Round 2 — BETWEEN + DELETE/UPDATE 인덱스 동기화

**내 작업 순서:**

1. **PR A (Phase 1 선행, 블로커 제거)** — 지금 진행 중 (`chore/round2-plan-and-interface`)
   - `include/types.h`: `WhereClause.value_to[256]`, `storage_select_result_by_row_indices` 선언 추가
   - `src/parser.c`: `BETWEEN A AND B` 파싱 → op="BETWEEN", value=A, value_to=B
   - `tests/test_parser.c`: BETWEEN 파싱 테스트 2건
   - CLAUDE.md / agent.md / claude_jiyong.md / README 업데이트
   - → 머지 후 정환·민철 착수 공지

2. **PR B (비교 벤치)**
   - `bench/benchmark.c`: 같은 데이터셋에서 선형 탐색 vs B+ 트리 조회 시간 비교
   - README 표에 "배율" 컬럼 추가

3. **PR C (Mix merge)**
   - 정환 `feature/executor-between` + 민철 `feature/storage-index-sync` 도착 시 Mix merge
   - CI green → dev 머지

4. **최종 dev → main** — 규태 MP5 머지 후

### PR 지시/관리 루틴
- 매 PR 에 사후 리뷰 코멘트 (잘된 점 + 개선점) 작성 — Round 1 에서 놓쳤던 부분 보완
- 팀원 작업 동기: **"Phase 1 PR(내 단독) 머지되면 즉시 Slack 공지"**

### MP5 — 웹 데모 (선택, 발표 전)

발표 메인 시연 컨텐츠: **결제/트랜잭션 로그 장애 구간 조회**
> 핵심 멘트: *"장애 발생 시 특정 시간 구간의 트랜잭션 로그를 빠르게 조회해야 한다 — B+Tree range query 가 O(log n + k) 로 해결."*

**리뷰 체크리스트:**
- [ ] 규태 `feature/web-demo` PR 도착 시 리뷰
- [ ] 본진 C 소스 / Makefile / 테스트 무변경 확인 (diff 범위 `web/` 만)
- [ ] `python3 web/server.py` 로 로컬 스모크 테스트:
    - [ ] `[ 더미 주입 ]` — `payments` 테이블 10만 건 생성
    - [ ] `[ 장애 구간 조회 ]` — `WHERE id BETWEEN A AND B` 1ms 내 반환
    - [ ] `[ 선형 vs 인덱스 비교 ]` — Chart.js 막대그래프 2개 표시
- [ ] 기존 `make test` / `make bench` 회귀 0 재확인 후 머지
- [ ] 본진 일정 위협하면 발표 후 머지로 연기

**FE↔본진 영향도 모니터링 (PM 책임):**
- 정환 `json_out.c` PR 머지 시 → 규태에게 `--json` 출력 스키마 diff 공유
- 민철 `storage.c` PR 머지 시 → auto-increment id 출력 위치 확인
- 지용 본인은 `bptree.c` 만 건드리므로 FE 영향 0

---

## 막히는 경우

### leaf split에서 부모 포인터 꼬임
```c
// split 후 부모에 키 삽입 순서
// 1. children 배열 오른쪽으로 밀기
// 2. keys 배열 오른쪽으로 밀기
// 3. 새 키/자식 삽입
// 4. num_keys++
```

### root split
```c
// 새 루트는 무조건 InternalNode
// 기존 루트 → left child
// split된 오른쪽 → right child
// 중간 키 → 새 루트의 keys[0]
```

### valgrind "still reachable"
- `bptree_destroy`에서 모든 노드 재귀 free 확인
- `LeafNode *next` 순회 free 확인
