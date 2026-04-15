#!/usr/bin/env python3
"""web/server.py — MiniSQL 결제 데모 중개 서버 (규태)

엔드포인트:
  GET  /              → index.html
  GET  /app.js        → app.js
  GET  /health        → 상태 확인
  POST /api/query     → sqlparser --json 실행
  POST /api/bench     → benchmark 실행 → stdout 파싱
  POST /api/inject    → 더미 결제 데이터 주입
  POST /api/range     → id 범위 조회 (bptree_range)
  POST /api/compare   → 선형 vs 인덱스 비교

의존성: Python stdlib 만 (외부 패키지 X)
"""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import json
import os
import random
import struct
import subprocess
import sys
import tempfile
import time

# 바이너리 레이아웃 — src/storage.c 의 bin_column_size 와 정확히 일치
_BIN_VARCHAR_LEN = 32
_BIN_PAYMENTS_FMT = f"<iii{_BIN_VARCHAR_LEN}si"

ROOT = Path(__file__).resolve().parent.parent
WEB_DIR = Path(__file__).resolve().parent
SQLPARSER = ROOT / "sqlparser"
BENCHMARK = ROOT / "benchmark"
TREE_SHAPE = ROOT / "tree_shape"

STATIC_FILES = {
    "/": ("index.html", "text/html; charset=utf-8"),
    "/index.html": ("index.html", "text/html; charset=utf-8"),
    "/app.js": ("app.js", "application/javascript; charset=utf-8"),
    "/cursor-trail.js": ("cursor-trail.js", "application/javascript; charset=utf-8"),
}


def run_sql(sql: str, timeout: int = 30, bulk: bool = False) -> dict:
    if not SQLPARSER.exists():
        return {"error": "sqlparser 바이너리 없음. make 먼저 실행하세요."}
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".sql", delete=False, encoding="utf-8"
    ) as tf:
        tf.write(sql)
        tmp = tf.name
    try:
        env = os.environ.copy()
        if bulk:
            # BULK_INSERT_MODE=1: per-insert fflush 생략으로 1M 건 INSERT 가 2.8초.
            # subprocess 모델이라 프로세스 종료 시 atexit 에서 flush + close.
            env["BULK_INSERT_MODE"] = "1"
        t0 = time.monotonic()
        proc = subprocess.run(
            [str(SQLPARSER), tmp, "--json"],
            capture_output=True, text=True, timeout=timeout, env=env,
        )
        elapsed_ms = (time.monotonic() - t0) * 1000
    except subprocess.TimeoutExpired:
        return {"error": f"sqlparser timeout ({timeout}s)"}
    finally:
        try:
            os.unlink(tmp)
        except OSError:
            pass

    statements = []
    pending = []

    def flush():
        if statements and pending:
            statements[-1]["_result"] = "\n".join(pending).rstrip()

    for line in proc.stdout.splitlines():
        s = line.strip()
        if not s:
            continue
        try:
            parsed = json.loads(s)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, dict) and "type" in parsed:
            flush()
            pending = []
            statements.append(parsed)
        else:
            pending.append(line)
    flush()

    return {
        "statements": statements,
        "stderr": proc.stderr,
        "returncode": proc.returncode,
        "elapsed_ms": round(elapsed_ms, 2),
    }


def run_bench(n: int = 1000000, order: int = 128, seed: int = 42) -> dict:
    if not BENCHMARK.exists():
        return {"error": "benchmark 바이너리 없음. make bench 먼저 실행하세요."}
    env = os.environ.copy()
    env["BENCH_N"] = str(n)
    env["BENCH_ORDER"] = str(order)
    env["BENCH_SEED"] = str(seed)
    try:
        proc = subprocess.run(
            [str(BENCHMARK)], capture_output=True, text=True, timeout=60, env=env,
        )
    except subprocess.TimeoutExpired:
        return {"error": "benchmark timeout"}

    result = {"raw": proc.stdout}
    for line in proc.stdout.splitlines():
        s = line.strip()
        # 단일 값 필드
        if s.startswith("INSERT"):
            parts = s.split("|")
            if len(parts) >= 3:
                result["insert_ops"] = parts[2].strip().split()[0]
        elif s.startswith("SEARCH"):
            parts = s.split("|")
            if len(parts) >= 3:
                result["search_ops"] = parts[2].strip().split()[0]
        elif s.startswith("RANGE"):
            parts = s.split("|")
            if len(parts) >= 3:
                result["range_qps"] = parts[2].strip().split()[0]
        elif s.startswith("VERIFY"):
            result["verify"] = s
        # 선형 vs 인덱스 비교 블록 파싱 — bench_compare 용도
        elif s.startswith("LINEAR"):
            parts = s.split("|")
            if len(parts) >= 2:
                try:
                    result["bench_linear_s"] = float(parts[0].split()[1])
                except (ValueError, IndexError):
                    pass
        elif s.startswith("INDEX"):
            parts = s.split("|")
            if len(parts) >= 2:
                try:
                    result["bench_index_s"] = float(parts[0].split()[1])
                except (ValueError, IndexError):
                    pass
        elif s.startswith("SPEEDUP"):
            try:
                result["bench_speedup"] = float(s.split()[1].rstrip("x"))
            except (ValueError, IndexError):
                pass
    return result


def inject_payments(count: int = 100000) -> dict:
    """더미 결제 로그 생성.
    - count <= 200_000: SQL INSERT 경로 (BULK_INSERT_MODE)
    - count >  200_000: Python 이 schema+CSV 파일을 직접 작성 (SQL 경로 우회).
      대용량에선 sqlparser 파싱/executor 호출 비용이 지배적이므로
      CSV 를 바로 쓰는 편이 수 배 빠름. 첫 SELECT 시 storage_ensure_index 가
      CSV → B+ 트리 lazy rebuild 수행."""
    t0 = time.monotonic()
    mode = "sql-bulk" if count <= 200_000 else "csv-direct"

    if mode == "sql-bulk":
        random.seed(42)
        lines = [
            "CREATE TABLE payments (id INT, user_id INT, amount INT, status VARCHAR, created_at INT);",
        ]
        base_ts = 1700000000
        # SQL 라인 생성과 같은 루프에서 BIN 바이트도 축적해 둔다. 같은 random
        # 시퀀스를 두 번 돌리지 않도록 한 루프에서 처리.
        bin_buf = bytearray()
        for i in range(1, count + 1):
            uid = random.randint(1000, 9999)
            amt = random.randint(100, 500000)
            r = random.random()
            status = "TIMEOUT" if r < 0.02 else ("FAIL" if r < 0.07 else "SUCCESS")
            ts = base_ts + i * 3
            lines.append(
                f"INSERT INTO payments (id, user_id, amount, status, created_at) "
                f"VALUES ({i}, {uid}, {amt}, '{status}', {ts});"
            )
            bin_buf += struct.pack(
                _BIN_PAYMENTS_FMT,
                i, uid, amt,
                status.encode("ascii").ljust(_BIN_VARCHAR_LEN, b"\0"),
                ts,
            )
        sql = "\n".join(lines)
        result = run_sql(sql, timeout=180, bulk=True)
        error = result.get("error")
        # SQL INSERT 성공 시 BIN 을 함께 기록 — SELECT 경로가 O(K) fseek 을 탐.
        # CSV 는 sqlparser 가 썼고, BIN 은 동일 데이터를 여기서 append 모드로 저장.
        if error is None:
            try:
                bin_path = ROOT / "data" / "tables" / "payments.bin"
                bin_path.parent.mkdir(parents=True, exist_ok=True)
                with bin_path.open("wb") as bf:
                    bf.write(bytes(bin_buf))
            except OSError as e:
                error = f"BIN 저장 실패 (SQL 은 성공): {e}"
    else:
        error = _write_payments_csv_direct(count)

    elapsed = (time.monotonic() - t0) * 1000
    return {
        "count": count,
        "mode": mode,
        "elapsed_ms": round(elapsed, 1),
        "error": error,
    }


def _write_payments_csv_direct(count: int) -> "str | None":
    """schema + CSV + 고정폭 BIN 세 파일을 Python 이 직접 작성. sqlparser 완전 우회.
    BIN 이 있으면 storage_select_result_by_row_indices 가 O(K) fseek 경로를 탐."""
    try:
        random.seed(42)
        schema_dir = ROOT / "data" / "schema"
        tables_dir = ROOT / "data" / "tables"
        schema_dir.mkdir(parents=True, exist_ok=True)
        tables_dir.mkdir(parents=True, exist_ok=True)

        (schema_dir / "payments.schema").write_text(
            "id,INT\nuser_id,INT\namount,INT\nstatus,VARCHAR\ncreated_at,INT\n",
            encoding="utf-8",
        )
        base_ts = 1700000000
        csv_path = tables_dir / "payments.csv"
        bin_path = tables_dir / "payments.bin"
        with csv_path.open("w", encoding="utf-8") as cf, \
             bin_path.open("wb") as bf:
            for i in range(1, count + 1):
                uid = random.randint(1000, 9999)
                amt = random.randint(100, 500000)
                r = random.random()
                status = "TIMEOUT" if r < 0.02 else ("FAIL" if r < 0.07 else "SUCCESS")
                ts = base_ts + i * 3
                cf.write(f"{i},{uid},{amt},{status},{ts}\n")
                bf.write(struct.pack(
                    _BIN_PAYMENTS_FMT,
                    i, uid, amt,
                    status.encode("ascii").ljust(_BIN_VARCHAR_LEN, b"\0"),
                    ts,
                ))
        return None
    except OSError as e:
        return f"CSV/BIN 직접 작성 실패: {e}"


def range_query(lo: int, hi: int) -> dict:
    """id 범위 조회 — bptree_range 사용.
    executor 는 where_count == 1 AND op == "BETWEEN" 에서만 B+ 트리 range 경로로
    분기하므로, `>= AND <=` 가 아닌 BETWEEN 으로 보내야 O(log n + k) 가 실측된다. """
    sql = f"SELECT * FROM payments WHERE id BETWEEN {lo} AND {hi};"
    t0 = time.monotonic()
    result = run_sql(sql, timeout=10)
    elapsed = (time.monotonic() - t0) * 1000

    rows = []
    for stmt in result.get("statements", []):
        if stmt.get("_result"):
            text = stmt["_result"]
            lines = text.split("\n")
            if len(lines) > 1:
                header = [h.strip() for h in lines[0].split("|")]
                for line in lines[1:]:
                    if line.startswith("(") and line.endswith(")"):
                        continue
                    cells = [c.strip() for c in line.split("|")]
                    if len(cells) == len(header):
                        rows.append(dict(zip(header, cells)))
    return {
        "lo": lo,
        "hi": hi,
        "row_count": len(rows),
        "rows": rows[:100],
        "elapsed_ms": round(elapsed, 2),
    }


def run_tree_shape(n: int = 20, order: int = 4, snapshots: int = 0) -> dict:
    """./tree_shape 실행 → bptree_print stdout 반환.
    snapshots>0 이면 중간 단계 스냅샷들을 함께 담아 FE 에서 replay 가능하게 한다."""
    if not TREE_SHAPE.exists():
        return {"error": "tree_shape 바이너리 없음. `make tree_shape` 실행하세요."}
    if n < 1: n = 1
    if n > 200: n = 200
    if order < 3: order = 4
    if order > 32: order = 32
    if snapshots < 0: snapshots = 0
    if snapshots > 20: snapshots = 20  # 브라우저 replay 예산

    args = [str(TREE_SHAPE), str(n), str(order)]
    if snapshots > 0:
        args += ["--snapshots", str(snapshots)]

    try:
        proc = subprocess.run(args, capture_output=True, text=True, timeout=5)
    except subprocess.TimeoutExpired:
        return {"error": "tree_shape timeout"}
    return {
        "n": n,
        "order": order,
        "snapshots": snapshots,
        "output": proc.stdout,
        "stderr": proc.stderr,
        "error": None if proc.returncode == 0 else f"exit {proc.returncode}",
    }


def compare_search(lo: int, hi: int) -> dict:
    """선형 vs 인덱스 비교. id 조건(인덱스) vs status 조건(선형)."""
    # 인덱스 검색 (id BETWEEN — executor 가 bptree_range 경로로 분기하는 유일한 형태)
    sql_index = f"SELECT * FROM payments WHERE id BETWEEN {lo} AND {hi};"
    t0 = time.monotonic()
    r1 = run_sql(sql_index, timeout=10)
    index_ms = (time.monotonic() - t0) * 1000

    # 선형 검색 (status 조건)
    sql_linear = "SELECT * FROM payments WHERE status = 'FAIL';"
    t0 = time.monotonic()
    r2 = run_sql(sql_linear, timeout=30)
    linear_ms = (time.monotonic() - t0) * 1000

    speedup = linear_ms / index_ms if index_ms > 0 else 0
    return {
        "index_ms": round(index_ms, 2),
        "linear_ms": round(linear_ms, 2),
        "speedup": round(speedup, 1),
        "index_error": r1.get("error"),
        "linear_error": r2.get("error"),
    }


class Handler(BaseHTTPRequestHandler):
    def _send(self, status, body, content_type):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, status, payload):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self._send(status, body, "application/json; charset=utf-8")

    def _read_body(self):
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            return None
        raw = self.rfile.read(length)
        try:
            return json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        if self.path in STATIC_FILES:
            fname, ctype = STATIC_FILES[self.path]
            fpath = WEB_DIR / fname
            if not fpath.exists():
                self._json(404, {"error": f"{fname} not found"})
                return
            self._send(200, fpath.read_bytes(), ctype)
            return
        if self.path == "/health":
            self._json(200, {
                "ok": True,
                "sqlparser": SQLPARSER.exists(),
                "benchmark": BENCHMARK.exists(),
            })
            return
        self._json(404, {"error": "not found"})

    def do_POST(self):
        data = self._read_body() or {}

        if self.path == "/api/query":
            sql = data.get("sql", "").strip()
            if not sql:
                self._json(400, {"error": "missing sql"})
                return
            self._json(200, run_sql(sql))

        elif self.path == "/api/bench":
            n = data.get("n", 1000000)
            order = data.get("order", 128)
            seed = data.get("seed", 42)
            self._json(200, run_bench(n, order, seed))

        elif self.path == "/api/inject":
            count = data.get("count", 100000)
            self._json(200, inject_payments(count))

        elif self.path == "/api/range":
            lo = data.get("lo", 30000)
            hi = data.get("hi", 31500)
            self._json(200, range_query(lo, hi))

        elif self.path == "/api/compare":
            lo = data.get("lo", 30000)
            hi = data.get("hi", 31500)
            self._json(200, compare_search(lo, hi))

        elif self.path == "/api/bench_compare":
            # 순수 자료구조 레벨 선형 vs 인덱스 비교 — bench 의 compare 섹션만 사용.
            # 기본 N=1,000,000, compare M=1,000 (benchmark.c 기본값과 동일).
            n = data.get("n", 1000000)
            order = data.get("order", 128)
            seed = data.get("seed", 42)
            res = run_bench(n, order, seed)
            # run_bench 에서 추출한 bench_* 필드만 꺼내 반환 (FE 데이터 축약)
            self._json(200, {
                "n": n,
                "m": 1000,
                "linear_s": res.get("bench_linear_s"),
                "index_s": res.get("bench_index_s"),
                "speedup": res.get("bench_speedup"),
                "error": res.get("error"),
            })

        elif self.path == "/api/tree_shape":
            # C 섹션 용 — B+ 트리 insert + bptree_print 라이브 출력
            n = int(data.get("n", 20))
            order = int(data.get("order", 4))
            snapshots = int(data.get("snapshots", 0))
            self._json(200, run_tree_shape(n, order, snapshots))

        else:
            self._json(404, {"error": "not found"})

    def log_message(self, fmt, *args):
        sys.stderr.write(f"[web] {self.address_string()} {fmt % args}\n")


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"[web] Payment Demo on http://localhost:{port}")
    print(f"[web] sqlparser: {'OK' if SQLPARSER.exists() else 'NOT FOUND'}")
    print(f"[web] benchmark: {'OK' if BENCHMARK.exists() else 'NOT FOUND'}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[web] bye")


if __name__ == "__main__":
    main()
