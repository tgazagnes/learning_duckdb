'''
Silver stage : clean data and store in silver.duckdb

Transformations : 
- join zones
- filter columns
- filter 0 values (remove trips with 0 passengers, 0 distance, 0 fare)
- calculate trip distance
- 
'''
import duckdb as d
import time

def transform_bronze_to_silver(duckdb_path, raw_trips_table, raw_zones_table, silver_table_name):
    # Connect to the bronze DuckDB
    con = d.connect(duckdb_path)

    # Clean raw trips data
    clean_trips = con.sql(
        f"""
            SELECT t.*,
                p.Zone AS Pickup_Zone,
                d.Zone AS Dropoff_Zone,
            FROM {raw_trips_table} AS t
            JOIN {raw_zones_table} AS p
                ON t.PULocationID = p.LocationID
            JOIN {raw_zones_table} AS d
                ON t.DOLocationID = d.LocationID
            WHERE passenger_count > 0 
                AND trip_distance > 0 
                AND fare_amount > 0
                AND tpep_pickup_datetime > '2025-01-01'
        """
    )
        
     

    # Create table 'trips_silver' in the current connection
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {silver_table_name} AS SELECT * FROM clean_trips
        """)

    # Close the silver connection
    con.close() 


if __name__ == "__main__":
    duckdb_path = '04_SmolETL/NYC_yellow_taxi.duckdb'
    raw_trips_table = 'trips_raw'
    raw_zones_table = 'zones_raw'
    silver_table_name = 'trips_silver'

    start_time = time.time()
    transform_bronze_to_silver(duckdb_path, raw_trips_table,raw_zones_table, silver_table_name)
    print("--- Silver stage done %.2f seconds ---" % (time.time() - start_time))


