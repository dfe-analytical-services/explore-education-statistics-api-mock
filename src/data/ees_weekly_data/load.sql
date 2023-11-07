COPY time_periods FROM '/home/nick/Development/personal/ees-public-api-mock/src/data/ees_weekly_data/time_periods.parquet' (FORMAT 'parquet', CODEC 'ZSTD');
COPY locations FROM '/home/nick/Development/personal/ees-public-api-mock/src/data/ees_weekly_data/locations.parquet' (FORMAT 'parquet', CODEC 'ZSTD');
COPY filters FROM '/home/nick/Development/personal/ees-public-api-mock/src/data/ees_weekly_data/filters.parquet' (FORMAT 'parquet', CODEC 'ZSTD');
COPY indicators FROM '/home/nick/Development/personal/ees-public-api-mock/src/data/ees_weekly_data/indicators.parquet' (FORMAT 'parquet', CODEC 'ZSTD');
COPY "data" FROM '/home/nick/Development/personal/ees-public-api-mock/src/data/ees_weekly_data/data.parquet' (FORMAT 'parquet', CODEC 'ZSTD');
