'''
Load, filter and aggregate a Parquet file with DuckDB, filter and aggregate data.
Measure the time taken for loading, filtering and aggregating.
'''

import duckdb as d
import time
import pandas as pd
from pathlib import Path
from memory_profiler import profile

# Define file paths
local_parquet_file = Path("00_data_sources/yellow_tripdata_full_2024.parquet")

@profile
def load_filter_aggregate_duckdb():
    # Connect to DuckDB (in-memory)
    con = d.connect(':memory:')
    start_time = time.time()
    # Load Parquet file into DuckDB
    con.execute(f"CREATE TABLE trips AS SELECT * FROM read_parquet('{local_parquet_file}')")
    duckdb_load_time = time.time() - start_time
    # Filter and aggregate data
    start_time = time.time()
    query = """
        SELECT
            DATE_TRUNC('month', tpep_pickup_datetime) AS month,
            COUNT(*) AS trip_count,
            AVG(trip_distance) AS avg_trip_distance
        FROM trips
        WHERE trip_distance > 0
        GROUP BY month
        ORDER BY month;
    """
    result = con.execute(query).fetchdf()
    duckdb_query_time = time.time() - start_time    
    return result, duckdb_load_time, duckdb_query_time

if __name__ == "__main__":
    result, duckdb_load_time, duckdb_query_time = load_filter_aggregate_duckdb()
    print(f"DuckDB Load Time: {duckdb_load_time:.2f} seconds")
    print(f"DuckDB Query Time: {duckdb_query_time:.2f} seconds")
    print("DuckDB Result:")
    print(result)


