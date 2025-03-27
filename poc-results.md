# Snowpark vs PostgreSQL Benchmark Results

## Executive Summary

This report presents the results of performance benchmarks comparing PostgreSQL to Snowflake's Hybrid Tables for high-throughput insert operations. Due to missing Snowflake authentication configuration, we were only able to run the PostgreSQL benchmarks. The benchmark focused on inserting 100,000 records with a target insert rate of 3,000 records per second.

## Test Environment

- **Hardware**: MacBook Pro (M-series chip)
- **PostgreSQL Version**: PostgreSQL running on port 5433
- **Testing Framework**: Custom Python benchmark tool using asyncio and connection pooling
- **Operation Tested**: Bulk inserts of 100,000 records with complex transaction data

## PostgreSQL Results

We ran 5 identical tests with PostgreSQL, each inserting 100,000 records. Here are the consolidated results:

| Test Run | Total Records | Actual Records Inserted | Total Time (seconds) | Insert Time (seconds) | Average Inserts/Second |
|----------|---------------|-------------------------|----------------------|-----------------------|------------------------|
| Run 1    | 100,000       | 100,000                 | 38.16                | 304.93                | 2,623.06               |
| Run 2    | 100,000       | 100,000                 | 36.87                | 293.32                | 2,714.88               |
| Run 3    | 100,000       | 100,000                 | 37.75                | 299.63                | 2,656.40               |
| Run 4    | 100,000       | 100,000                 | 33.10                | 265.55                | 3,020.51               |
| Run 5    | 100,000       | 100,000                 | 35.91                | 285.81                | 2,784.03               |
| **Average** | 100,000    | 100,000                 | **36.36**            | **289.85**            | **2,759.78**           |

## Performance Analysis

### PostgreSQL Performance

- **Throughput**: PostgreSQL achieved an average of ~2,760 inserts per second, which is 92% of the target rate of 3,000 inserts per second.
- **Consistency**: Performance was generally consistent across test runs, with Run 4 achieving the fastest time and exceeding our target.
- **Scalability**: PostgreSQL handled concurrent inserts effectively using connection pooling, maintaining good throughput.

### Notable Observations

1. **Insert Time vs. Total Time**: The insert time (~290 seconds) is significantly higher than the total time (~36.4 seconds) because the benchmark utilizes connection pooling and parallelism. The insert time represents the cumulative time across all parallel operations.

2. **Concurrency Benefits**: PostgreSQL shows good performance with concurrent operations, efficiently utilizing the connection pool to maintain a high throughput rate.

3. **Progress Rate**: The inserts maintained a fairly consistent rate throughout the test runs, with some variation between runs (best performance in Run 4).

## Conclusion

PostgreSQL demonstrates robust and consistent performance for high-throughput insert operations, achieving approximately 92% of the target insert rate. The database showed stable behavior across multiple test runs with some variations in performance metrics.

Unfortunately, we couldn't compare these results with Snowflake's Hybrid Tables due to missing authentication configuration. A complete comparison would require setting up proper Snowflake credentials and running equivalent benchmarks on both platforms.

## Recommendations

1. **PostgreSQL Optimization**: While PostgreSQL performed well, there may be room for optimization through tuning of connection pool parameters, transaction management, or PostgreSQL server configuration.

2. **Complete the Comparison**: To make a fair assessment between PostgreSQL and Snowflake, configure the Snowflake connection and rerun the benchmarks on both platforms.

3. **Extended Testing**: Consider testing with larger volumes (1M+ records) and different operation types (queries, updates, deletes) to provide a more comprehensive performance comparison. 