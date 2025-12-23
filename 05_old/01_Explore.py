import duckdb as d

# Connect to an in-memory database
con = d.connect(database=':memory:', read_only=False)

# The duckdb module is often imported as 'd' in examples, but we'll use 'duckdb' for clarity
parquet_path = '00_Data_sources/yellow_tripdata_2023-01.parquet'

# Read the Parquet file into a Relation
taxi_data = d.read_parquet(parquet_path)

# Print the first 5 rows (materializes to a Pandas DataFrame by default in the REPL)
print(taxi_data.limit(5))

# The SQL API is the most flexible way to start a query
result_df = con.sql(f"SELECT * FROM '{parquet_path}' LIMIT 5").df()

# Print the resulting Pandas DataFrame
print(result_df)

# Example Analytical Query: Find the average trip distance and total fare
summary = con.sql(f"""
    SELECT
        COUNT(*) AS total_trips,
        AVG(trip_distance) AS avg_distance_miles,
        SUM(total_amount) AS total_revenue
    FROM '{parquet_path}'
    WHERE total_amount > 0 AND trip_distance > 0;
""").df()

print("\n--- Summary Statistics ---")
print(summary)

con.close()