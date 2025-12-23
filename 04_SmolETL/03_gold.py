'''
Gold stage : Provide ready to use data for analysis and ML

Source : trips_silver

Target Tables :
- facts_trips_daily
- dim_zones
- dim_rates
- dim_payment_types
- dim_datetime 

'''
import duckdb as d
import time

def transform_silver_to_gold(duckdb_path: str):
    # Connect to DuckDB
    con = d.connect(duckdb_path)

    # Create facts_trips_daily table
    con.execute(
        """
        CREATE OR REPLACE TABLE facts_trips_daily AS
        SELECT 
            DATE_TRUNC('day', t.tpep_pickup_datetime) AS trip_date,
            COUNT(*) AS total_trips,
            SUM(t.fare_amount) AS total_fare_amount,
            SUM(t.tip_amount) AS total_tip_amount,
            AVG(t.trip_distance) AS avg_trip_distance
        FROM trips_silver AS t
        GROUP BY trip_date;
        """
    )

    # Close the gold connection
    con.close()

if __name__ == "__main__":
    duckdb_path = '04_SmolETL/NYC_yellow_taxi.duckdb'

    start_time = time.time()
    transform_silver_to_gold(duckdb_path)
    print("--- Gold stage done %.2f seconds ---" % (time.time() - start_time))