'''
Concatenate and join data from multiple Parquet files using Polars
'''

import polars as pl
import matplotlib.pyplot as plt

## Sales fact + products dimension across Parquet folders
result_polars = pl.scan_parquet('00_data_sources/yellow_tripdata_2025-*.parquet') \
    .filter(pl.col('tpep_pickup_datetime') >= pl.datetime(2025, 3, 1)) \
    .select([
        'tpep_pickup_datetime', 'VendorID', 'passenger_count', 'trip_distance',
        'RatecodeID', pl.col('PULocationID').alias('LocationID'), 'total_amount'
    ]) \
    .join(
        pl.read_csv('00_data_sources/taxi_zone_lookup.csv').select([
            'LocationID', 'Borough', 'Zone', 'service_zone'
        ]),
        on='LocationID',
        how='left'
    ) \
    .groupby('Borough') \
    .agg(pl.round(pl.sum('total_amount')).alias('total_amount')) \
    .sort('total_amount', reverse=True) \
    .collect()

print(result_polars)
# load in pandas to calculate percentages
# df = result_polars.to_pandas()
# df.set_index('Borough', inplace=True)
# df['percentage'] = df['total_amount'] / df['total_amount'].sum() * 100
# df['percentage'] = df['percentage'].map('{:.0f}%'.format)
# # plot
# print(df.percentage)