"""Generate a minimal test Parquet file for the serverless ETL pipeline."""

import pyarrow as pa
import pyarrow.parquet as pq

table = pa.table({
    "id": pa.array([1, 2, 3], type=pa.int64()),
    "name": pa.array(["Alice", "Bob", "Charlie"], type=pa.string()),
    "amount": pa.array([100.50, 200.75, 350.00], type=pa.float64()),
})

pq.write_table(table, "test-data.parquet")
print(f"Written {table.num_rows} rows to test-data.parquet")
