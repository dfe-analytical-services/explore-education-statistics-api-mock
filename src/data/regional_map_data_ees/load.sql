COPY "data" FROM 'data.parquet' (FORMAT 'parquet', CODEC 'ZSTD');
COPY time_periods FROM 'time_periods.parquet' (FORMAT 'parquet', CODEC 'ZSTD');
COPY locations FROM 'locations.parquet' (FORMAT 'parquet', CODEC 'ZSTD');
COPY filters FROM 'filters.parquet' (FORMAT 'parquet', CODEC 'ZSTD');
COPY indicators FROM 'indicators.parquet' (FORMAT 'parquet', CODEC 'ZSTD');
COPY data_normalised FROM 'data_normalised.parquet' (FORMAT 'parquet', CODEC 'ZSTD');
