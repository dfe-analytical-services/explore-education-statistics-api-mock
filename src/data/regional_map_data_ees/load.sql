COPY data_temp FROM '/home/nick/Development/personal/ees-public-api-mock/src/data/regional_map_data_ees/data_temp.parquet' (FORMAT 'parquet', CODEC 'ZSTD');
COPY time_periods FROM '/home/nick/Development/personal/ees-public-api-mock/src/data/regional_map_data_ees/time_periods.parquet' (FORMAT 'parquet', CODEC 'ZSTD');
COPY locations FROM '/home/nick/Development/personal/ees-public-api-mock/src/data/regional_map_data_ees/locations.parquet' (FORMAT 'parquet', CODEC 'ZSTD');
COPY filters FROM '/home/nick/Development/personal/ees-public-api-mock/src/data/regional_map_data_ees/filters.parquet' (FORMAT 'parquet', CODEC 'ZSTD');
COPY indicators FROM '/home/nick/Development/personal/ees-public-api-mock/src/data/regional_map_data_ees/indicators.parquet' (FORMAT 'parquet', CODEC 'ZSTD');
COPY "data" FROM '/home/nick/Development/personal/ees-public-api-mock/src/data/regional_map_data_ees/data.parquet' (FORMAT 'parquet', CODEC 'ZSTD');
