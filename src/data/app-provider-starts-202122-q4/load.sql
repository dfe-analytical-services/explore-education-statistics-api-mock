COPY "data" FROM '/home/nick/Development/personal/ees-public-api-mock/src/data/app-provider-starts-202122-q4/data.parquet' (FORMAT 'parquet', CODEC 'ZSTD');
COPY time_periods FROM '/home/nick/Development/personal/ees-public-api-mock/src/data/app-provider-starts-202122-q4/time_periods.parquet' (FORMAT 'parquet', CODEC 'ZSTD');
COPY locations FROM '/home/nick/Development/personal/ees-public-api-mock/src/data/app-provider-starts-202122-q4/locations.parquet' (FORMAT 'parquet', CODEC 'ZSTD');
COPY filters FROM '/home/nick/Development/personal/ees-public-api-mock/src/data/app-provider-starts-202122-q4/filters.parquet' (FORMAT 'parquet', CODEC 'ZSTD');
COPY indicators FROM '/home/nick/Development/personal/ees-public-api-mock/src/data/app-provider-starts-202122-q4/indicators.parquet' (FORMAT 'parquet', CODEC 'ZSTD');
