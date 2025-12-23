'''
Bronze stage : load all parquet files into duckdb db
Note : fichiers en local pour le POC. Possible de les récupérer directement par Url via read_parquet('url')
'''
import duckdb as d
import pandas as pd
import time

def load_source_file_to_duckdb(file_type, filepattern, duckdb_path, table_name):
    start_time = time.time()

    # Connect to DuckDB (it will create the database file if it doesn't exist)
    conn = d.connect(duckdb_path)

    if file_type == 'parquet':
        sql_query = f"""
        CREATE OR REPLACE TABLE {table_name} AS 
        SELECT * FROM read_parquet('{filepattern}');
        """
    elif file_type == 'csv':
        sql_query = f"""
        CREATE OR REPLACE TABLE {table_name} AS 
        SELECT * FROM read_csv_auto('{filepattern}');
        """
    
    # Execute the single SQL string
    conn.execute(sql_query)
    
    # Close the connection
    conn.close()
    print("--- Done %.2f seconds ---" % (time.time() - start_time))



if __name__ == "__main__":
    # Load 2 source files into DuckDB
    load_source_file_to_duckdb(file_type='parquet',
                           filepattern = '00_data_sources/yellow_tripdata_2025-*.parquet',
                           duckdb_path = '04_SmolETL/NYC_yellow_taxi.duckdb',
                           table_name = 'trips_raw')
    load_source_file_to_duckdb(file_type='csv',
                           filepattern = '00_data_sources/taxi_zone_lookup.csv',
                           duckdb_path = '04_SmolETL/NYC_yellow_taxi.duckdb',
                           table_name = 'zones_raw')