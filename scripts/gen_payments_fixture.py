#!/usr/bin/env python3
"""scripts/gen_payments_fixture.py — 결제 로그 CSV 픽스처 직접 생성기.

sqlparser INSERT 경로 우회. Python 이 schema / CSV 파일을 바로 작성한다.
첫 SELECT 질의 시 sqlparser 의 storage_ensure_index 가 CSV → B+ 트리를 lazy rebuild.

사용법:
    python3 scripts/gen_payments_fixture.py            # 100만 건
    python3 scripts/gen_payments_fixture.py 10000000   # 1000만 건
    python3 scripts/gen_payments_fixture.py 1000000 --seed 7

출력:
    data/schema/payments.schema
    data/tables/payments.csv

규모별 예상 (WSL 9p 기준):
    100k  → 0.25s   (~5MB)
    1M    → 2.6s    (~37MB)
    10M   → 24s     (~368MB)
"""
import argparse
import random
import struct
import time
from pathlib import Path

# 바이너리 레이아웃 — src/storage.c 의 bin_column_size 와 정확히 일치해야 함.
# payments schema: id INT, user_id INT, amount INT, status VARCHAR, created_at INT
# row = 4 + 4 + 4 + 32 + 4 = 48 bytes
BIN_VARCHAR_LEN = 32
BIN_ROW_FMT = f"<iii{BIN_VARCHAR_LEN}si"  # little-endian


def main() -> None:
    ap = argparse.ArgumentParser(description="payments 더미 CSV 생성")
    ap.add_argument("count", nargs="?", type=int, default=1_000_000,
                    help="생성할 행 수 (기본 1,000,000)")
    ap.add_argument("--seed", type=int, default=42, help="랜덤 시드")
    ap.add_argument(
        "--root", type=Path, default=Path(__file__).resolve().parent.parent,
        help="프로젝트 루트 (기본: 이 스크립트의 상위 디렉터리)",
    )
    args = ap.parse_args()

    random.seed(args.seed)
    schema_dir = args.root / "data" / "schema"
    tables_dir = args.root / "data" / "tables"
    schema_dir.mkdir(parents=True, exist_ok=True)
    tables_dir.mkdir(parents=True, exist_ok=True)

    schema_path = schema_dir / "payments.schema"
    csv_path = tables_dir / "payments.csv"
    bin_path = tables_dir / "payments.bin"

    schema_path.write_text(
        "id,INT\nuser_id,INT\namount,INT\nstatus,VARCHAR\ncreated_at,INT\n",
        encoding="utf-8",
    )

    t0 = time.monotonic()
    base_ts = 1700000000
    # CSV 와 .bin 을 같은 데이터로 한 번에 작성
    with csv_path.open("w", encoding="utf-8") as cf, \
         bin_path.open("wb") as bf:
        for i in range(1, args.count + 1):
            uid = random.randint(1000, 9999)
            amt = random.randint(100, 500000)
            r = random.random()
            status = "TIMEOUT" if r < 0.02 else ("FAIL" if r < 0.07 else "SUCCESS")
            ts = base_ts + i * 3
            # CSV
            cf.write(f"{i},{uid},{amt},{status},{ts}\n")
            # 바이너리 — storage.c 의 decode_binary_row 와 정확히 호환
            bf.write(struct.pack(
                BIN_ROW_FMT,
                i, uid, amt,
                status.encode("ascii").ljust(BIN_VARCHAR_LEN, b"\0"),
                ts,
            ))
    elapsed = time.monotonic() - t0

    csv_mb = csv_path.stat().st_size / (1024 * 1024)
    bin_mb = bin_path.stat().st_size / (1024 * 1024)
    print(f"wrote {args.count:,} rows in {elapsed:.2f}s")
    print(f"  schema: {schema_path}")
    print(f"  csv:    {csv_path}  ({csv_mb:.1f} MB)")
    print(f"  bin:    {bin_path}  ({bin_mb:.1f} MB)  [O(K) fseek 경로용]")


if __name__ == "__main__":
    main()
