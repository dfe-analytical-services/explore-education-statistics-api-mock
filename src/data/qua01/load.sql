COPY "data" FROM '/home/nick/Development/personal/ees-public-api-mock/src/data/qua01/data.parquet' (FORMAT 'parquet', CODEC 'ZSTD');
COPY time_periods FROM '/home/nick/Development/personal/ees-public-api-mock/src/data/qua01/time_periods.parquet' (FORMAT 'parquet', CODEC 'ZSTD');
COPY locations FROM '/home/nick/Development/personal/ees-public-api-mock/src/data/qua01/locations.parquet' (FORMAT 'parquet', CODEC 'ZSTD');
COPY filters FROM '/home/nick/Development/personal/ees-public-api-mock/src/data/qua01/filters.parquet' (FORMAT 'parquet', CODEC 'ZSTD');
COPY indicators FROM '/home/nick/Development/personal/ees-public-api-mock/src/data/qua01/indicators.parquet' (FORMAT 'parquet', CODEC 'ZSTD');
