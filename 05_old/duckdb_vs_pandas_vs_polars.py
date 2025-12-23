import duckdb
import time
import json
import os
import matplotlib.pyplot as plt
import polars as pl
# Comparaison des performances de DuckDB et Polars pour le chargement et l'analyse de fichiers Parquet

# Charger et concaténer les fichiers Parquet avec DuckDB
start_time = time.time()
duckdb_con = duckdb.connect(":memory:")
parquet_filepaths_2025 = [
    f"00_data_sources/yellow_tripdata_2025-{str(month).zfill(2)}.parquet"
    for month in range(1, 10)
]
duckdb_con.sql("""
    CREATE TABLE full_taxi_data_2025 AS
    SELECT * FROM read_parquet("00_data_sources/yellow_tripdata_2025-*.parquet")
""")

execution_time_duckdb = time.time() - start_time

# Charger et concaténer les fichiers Parquet avec Polars
start_time = time.time()

polars_dfs = [pl.read_parquet(fp) for fp in parquet_filepaths_2025]
full_taxi_data_2025_polars = pl.concat(polars_dfs)

execution_time_polars = time.time() - start_time

print(f"Execution time Polars: {execution_time_polars:.4f} seconds")
print(f"Execution time DuckDB: {execution_time_duckdb:.4f} seconds")

# Plot comparison of execution times
labels = ['DuckDB', 'Polars']
times = [execution_time_duckdb, execution_time_polars]
plt.bar(labels, times, color=['blue', 'orange'])
plt.ylabel('Execution Time (seconds)')
plt.title('Execution Time Comparison: DuckDB vs Polars')
plt.show()

