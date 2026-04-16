# WIL — INSERT 최적화 여정과 Reverse Proxy 구조의 동형성

> 작성 맥락: B+ Tree 인덱스 INSERT 성능을 **32 ms → 3 µs (약 10,000배)** 개선한 여정을 돌아보다가, 이 최적화 흐름이 사실 우리가 매일 쓰는 **reverse proxy / 웹 인프라**의 설계 원칙과 동형(isomorphic)이라는 것을 발견했다.

---

## 1. 출발점 — 던진 질문

> "INSERT 성능 최적화 여정 (32 ms → 3 µs)이 웹 proxy 서버에서 reverse proxy 구조와 유사한 것 같은데, 어떻게 생각하시나요?"

단순히 "DB 인덱스 최적화를 했다"로 끝낼 수 있던 경험이,
질문을 다르게 던져보니 **"I/O 경계에서 반복되는 비싼 왕복을 어떻게 줄일 것인가"** 라는
일반화된 문제로 재해석됐다.

---

## 2. 시도한 접근 방식 — 5단계

### 문제 정의
- 초기: 60초 안에 100만 건 중 **2,716건만** 삽입 완료 (건당 32 ms, 추정 완료 8시간)
- 원인은 알고리즘이 아니라 **개발 환경의 파일 시스템 레이어**
  ```
  Windows Host → WSL2 → Docker → devcontainer (9p + dirsync)
  ```
  `fopen`, `fclose`, `stat` 한 번에 약 0.3 ms, 건당 6회 호출 → **1.8 ms / row** 가 I/O로 사라지고 있었다.

### 단계별 시도

| 단계 | 시도한 접근 | 근거 / 아이디어 | 효과 |
|---|---|---|---|
| ① | **파일 포인터 재사용** — 매 row마다 open/close 하던 것을 프로세스 생존 기간 동안 유지 + 64KB 버퍼 | setup 비용을 분산 (connection setup == fd setup) | 14.8 s → 13.5 s |
| ② | **스키마 캐싱** — 매 INSERT마다 읽던 스키마를 첫 1회만 로드 후 메모리 재사용 | 안 변하는 데이터는 다시 묻지 않는다 | 13.5 s → 4.8 s |
| ③ | **경로 캐싱** — 매번 탐색하던 파일 경로를 첫 호출에 확정 후 재사용 | 라우팅 결과도 캐시 대상이다 | 4.8 s → 1.4 s |
| ④ | **meta 캐싱** — 매 INSERT마다 O(N²) 재스캔하던 메타데이터 제거 | hot path의 중복 계산 제거 | O(N²) → O(1) |
| ⑤ | **BULK_INSERT_MODE** — 대량 삽입 시 fflush를 버퍼가 찰 때만 수행 | syscall을 묶어서 지연 flush | 1.4 s → **0.058 s** |

### 결과
| 모드 | 1M INSERT |
|---|---|
| 최적화 전 | ~8시간 (추정) |
| 최적화 후 (기본) | 138 s |
| BULK_INSERT_MODE | **2.8 s** |

→ 건당 **32 ms → 3 µs**, 약 **10,000배 개선**

---

## 3. 발견 — Reverse Proxy 구조와의 동형성

5단계 최적화를 나열하고 보니, 각 조치가 **nginx 같은 reverse proxy가 하는 일과 정확히 대응**된다는 것을 깨달았다.

| 우리 INSERT 최적화 | Reverse Proxy 대응 개념 |
|---|---|
| ① 파일 포인터 재사용 (매번 open/close X) | **keepalive** upstream connection pool |
| ② 스키마 캐싱 | **proxy_cache** (정적 메타/응답 캐싱) |
| ③ 경로 캐싱 | **resolver cache** / routing table |
| ④ meta 캐싱 (O(N²) 제거) | **microcache** (hot path 중복 제거) |
| ⑤ BULK_INSERT_MODE (fflush 지연) | **proxy_buffering** + write coalescing |

### 공통 철학 3가지

1. **Connection / Handle 재사용**
   - 매 요청마다 TCP handshake 하지 않는 것 == 매 row마다 fopen 하지 않는 것
   - setup 비용을 분산시키는 패턴

2. **계층별 캐싱**
   - 변하지 않는 데이터(스키마, 경로, upstream 응답)는 한 번만 묻는다
   - DB 인덱스의 buffer pool, OS의 page cache, CDN의 edge cache 모두 같은 원리

3. **Write batching / Lazy flush**
   - syscall · 네트워크 호출을 묶어서 지연 flush
   - `proxy_buffering on`, TCP Nagle, fsync 지연, WAL group commit — 전부 같은 가족

---

## 4. 배운 것 (Takeaway)

1. **"병목은 알고리즘이 아니라 I/O 경계였다."**
   O(log n) 트리 구조를 아무리 잘 짜도, 건당 6번의 syscall이 1.8 ms를 먹으면 의미가 없다.
   성능 문제를 만났을 때 "알고리즘 복잡도 / 시스템 레이어 / 환경 레이어" 중 어디가 진짜 범인인지
   **측정부터 하는 습관**이 필요하다는 걸 체감했다.

2. **최적화 패턴은 도메인을 넘어 재사용된다.**
   DB · 웹 서버 · OS · 네트워크 — 모두 "느린 경계를 어떻게 넘을 것인가"라는 같은 문제를 풀고 있다.
   한 도메인에서 쓴 패턴(caching, pooling, batching)은 다른 도메인에서도 그대로 작동한다.
   앞으로 새로운 영역을 공부할 때 **"이건 내가 아는 어떤 패턴과 동형인가?"** 를 먼저 묻게 될 것 같다.

3. **좋은 질문이 경험의 가치를 바꾼다.**
   "INSERT 최적화를 했다"는 단순 사실이,
   "이건 reverse proxy와 같은 구조 아닌가?" 라는 질문 하나로
   **일반화된 시스템 설계 원칙**으로 재해석됐다.
   구현한 내용을 다른 추상화 층위로 **다시 묻는 습관**이 WIL의 핵심이라는 걸 배웠다.

---

## 5. 다음에 더 시도해보고 싶은 것

- **Write-Ahead Log (WAL) 도입** — BULK_INSERT_MODE의 일반화. 지연 flush를 안전하게 하는 표준 기법.
- **mmap 기반 storage** — 파일 포인터 재사용의 극한. 커널 page cache를 직접 공유.
- **io_uring 비교 벤치** — syscall batching을 OS 레이어에서 풀면 얼마나 더 빨라질지.
- 이 세 가지 모두 **"I/O 경계를 어떻게 넘느냐"** 라는 같은 질문의 확장이다.

---

*작성: 2026-04-16 / MiniSQL Week 7 — B+ Tree Index 프로젝트 회고 중*
