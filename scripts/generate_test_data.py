"""Generate test Parquet files for the serverless ETL pipeline.

Writes N small files split across two tables (`test_data` and `customers`) into a
local output folder. File names carry the table's mapping stem so the enrichment
Lambda's file_mapping patterns resolve each file to the right Iceberg table:

    test-data-<i>.parquet   -> test_data
    customers-<i>.parquet   -> customers

Then upload the whole folder at once with:  ./scripts/trigger.sh <folder>
"""
import argparse
import os
import random
import string

import pyarrow as pa
import pyarrow.parquet as pq

FIRST_NAMES = ["Alice", "Bob", "Charlie", "Dana", "Evan", "Fiona", "Gus", "Hana"]
REGIONS = ["us-east-2", "us-west-2", "eu-west-1", "ap-southeast-1"]


def _rand_email() -> str:
    user = "".join(random.choices(string.ascii_lowercase, k=8))
    return f"{user}@example.com"


def make_test_data(n_rows: int, start_id: int) -> pa.Table:
    return pa.table({
        "id": pa.array(list(range(start_id, start_id + n_rows)), type=pa.int64()),
        "name": pa.array(random.choices(FIRST_NAMES, k=n_rows), type=pa.string()),
        "amount": pa.array(
            [round(random.uniform(10, 1000), 2) for _ in range(n_rows)],
            type=pa.float64(),
        ),
    })


def make_customers(n_rows: int, start_id: int) -> pa.Table:
    return pa.table({
        "customer_id": pa.array(list(range(start_id, start_id + n_rows)), type=pa.int64()),
        "email": pa.array([_rand_email() for _ in range(n_rows)], type=pa.string()),
        "region": pa.array(random.choices(REGIONS, k=n_rows), type=pa.string()),
    })


# Which table each generated file targets, by its filename stem. The stem must
# match the enrichment Lambda's file_mapping patterns.
TABLES = {
    "test-data": make_test_data,
    "customers": make_customers,
}


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate small test parquet files across two tables.")
    ap.add_argument("--out", default="test-batch", help="Output folder (default: test-batch)")
    ap.add_argument("--count", type=int, default=10, help="Number of files to generate (default: 10)")
    ap.add_argument("--min-rows", type=int, default=100, help="Min rows per small file (default: 100)")
    ap.add_argument("--max-rows", type=int, default=200, help="Max rows per small file (default: 200)")
    ap.add_argument("--big-rows", type=int, default=1000, help="Row count for the big files (default: 1000)")
    ap.add_argument("--big-count", type=int, default=2, help="How many of the files are big (default: 2)")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    stems = list(TABLES.keys())

    # Spread the big files across the batch (e.g. indices 3 and 7 of 10) so a
    # window naturally mixes small and big — the small ones pack together, the
    # big ones cross the size threshold into their own EMR jobs.
    big_indices = set()
    if args.big_count > 0:
        step = max(1, args.count // (args.big_count + 1))
        big_indices = {min((k + 1) * step, args.count - 1) for k in range(args.big_count)}

    for i in range(args.count):
        # Alternate tables so a batch fans out to both Iceberg tables.
        stem = stems[i % len(stems)]
        is_big = i in big_indices
        n_rows = args.big_rows if is_big else random.randint(args.min_rows, args.max_rows)
        start_id = i * 100000 + 1
        table = TABLES[stem](n_rows, start_id)
        path = os.path.join(args.out, f"{stem}-{i:02d}.parquet")
        pq.write_table(table, path)
        tag = " [BIG]" if is_big else ""
        print(f"  {path}: {n_rows} rows -> table '{stem.replace('-', '_')}'{tag}")

    print(f"\nWrote {args.count} files to {args.out}/ ({len(big_indices)} big). Sizes:")
    for f in sorted(os.listdir(args.out)):
        p = os.path.join(args.out, f)
        print(f"  {os.path.getsize(p):>8,} bytes  {f}")
    print("\nSet BatchSizeThresholdBytes between the small and big sizes above.")
    print(f"Upload them with:\n  ./scripts/trigger.sh {args.out}")


if __name__ == "__main__":
    main()
