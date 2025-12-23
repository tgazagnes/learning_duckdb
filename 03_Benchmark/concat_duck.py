import duckdb as d
import matplotlib.pyplot as plt

## Sales fact + products dimension across Parquet folders
result_duckdb = d.sql(
    """

    WITH trips AS (
    SELECT tpep_pickup_datetime, VendorID, passenger_count, trip_distance, RatecodeID, PULocationID as LocationID, total_amount,  
    FROM '00_data_sources/yellow_tripdata_2025-*.parquet'
    WHERE tpep_pickup_datetime >= DATE '2025-03-01'
    ),
    zones AS (
    SELECT LocationID,Borough,Zone,service_zone
    FROM '00_data_sources/taxi_zone_lookup.csv'
    )
    SELECT
    zones.Borough AS borough, 
    ROUND(SUM(trips.total_amount)) AS total_amount,
    FROM trips
    LEFT JOIN zones USING (LocationID)
    GROUP BY 1
    ORDER BY 2 DESC;

    """
)

print(result_duckdb)
# load in pandas to calculate percentages

df = result_duckdb.df()
df.set_index('borough', inplace=True)
df['percentage'] = df['total_amount'] / df['total_amount'].sum() * 100
df['percentage'] = df['percentage'].map('{:.0f}%'.format)
# plot
print(df.percentage)
