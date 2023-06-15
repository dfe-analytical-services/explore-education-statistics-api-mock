COPY indicators FROM '/home/nick/Development/personal/ees-public-api-mock/src/data/spc_pupils_ethnicity_and_language_/indicators.parquet' (FORMAT 'parquet', CODEC 'ZSTD');
COPY filters FROM '/home/nick/Development/personal/ees-public-api-mock/src/data/spc_pupils_ethnicity_and_language_/filters.parquet' (FORMAT 'parquet', CODEC 'ZSTD');
COPY locations FROM '/home/nick/Development/personal/ees-public-api-mock/src/data/spc_pupils_ethnicity_and_language_/locations.parquet' (FORMAT 'parquet', CODEC 'ZSTD');
COPY time_periods FROM '/home/nick/Development/personal/ees-public-api-mock/src/data/spc_pupils_ethnicity_and_language_/time_periods.parquet' (FORMAT 'parquet', CODEC 'ZSTD');
COPY "data" FROM '/home/nick/Development/personal/ees-public-api-mock/src/data/spc_pupils_ethnicity_and_language_/data.parquet' (FORMAT 'parquet', CODEC 'ZSTD');
