# Snowflake Hybrid Tables Join Benchmark

## Overview

This benchmark tests the performance of joining a standard Snowflake table with a hybrid table using watermarks to filter data based on timestamps. The use case simulates a data pipeline that processes new records since the last run, tracking progress using watermark dates.

## Setup Instructions

### Prerequisites

- Python 3.11+
- [UV](https://github.com/astral-sh/uv) (modern Python package manager)
- Snowflake account with access to the FINALOOP database

### Snowflake Connection Configuration

1. Create a `~/.snowflake/connections.toml` file with your Snowflake connection details:

```toml
[hybrid_table_poc]
account = "your-snowflake-account"
user = "your-username"
password = "your-password"  # Optional if using SSO
authenticator = "externalbrowser"  # Use for SSO authentication
warehouse = "your-warehouse" 
role = "your-role"
database = "FINALOOP"
schema = "PUBLIC"
```

> Note: If you prefer to use a different connection name, update the `connection_name` parameter in the `SnowflakeHybridTablesBenchmark` class.

### Installing Dependencies

From the project root directory:

```bash
uv add snowflake-connector-python
```

### Running the Benchmark

To run the benchmark:

```bash
cd /path/to/project
uv run src/join_benchmark/snowflake_hybrid_tables_benchmark.py
```

The benchmark will:
1. Create a hybrid table for watermarks
2. Generate and insert watermark data
3. Run queries using both approaches (hybrid table and temporary table)
4. Print the results to the console

## What We Tested

1. **Tables**:
   - **Business Table**: Used existing `FINALOOP.MODELS.AOV_AOGP_HISTOGRAM` table
   - **Watermarks Table**: Created a hybrid table `FINALOOP.PUBLIC.WATERMARKS`

2. **Join Operations**:
   - Join condition: `business_table.COMPANY_ID = watermarks_table.COMPANY_ID`
   - Filter criteria: `business_table.ORDER_CREATED_DATE > watermarks_table.LAST_PROCESSED_DATE`
   - Operation: COUNT(*) to measure pure query performance without data transfer overhead

3. **Watermark Strategy**:
   - Set fixed watermark date of January 1, 2020 for all companies
   - Used all available companies (1,094 unique companies)
   - No pipeline IDs (simplification to one watermark per company)

4. **Alternative Approach**:
   - **Hybrid Tables**: Storing watermarks in a dedicated hybrid table
   - **SPLIT_TO_TABLE**: Using a temporary table approach with batch inserts instead of the original SPLIT_TO_TABLE function (which encountered limitations with large inputs)

## Results

### Hybrid Tables Approach

We ran 10 iterations of the join query with hybrid tables and found:

- **Data Volume**: 118,288,140 matching records processed in the join
- **Average Query Time**: 1.77 seconds
- **Min Query Time**: 1.68 seconds
- **Max Query Time**: 1.87 seconds

### Temporary Table Approach

We also ran 10 iterations of the alternative approach using temporary tables:

- **Data Volume**: 118,288,140 matching records processed in the join
- **Average Query Time**: 9.23 seconds
- **Min Query Time**: 8.30 seconds
- **Max Query Time**: 10.20 seconds

### Comparison

| Metric | Hybrid Tables | Temp Table Approach | Difference |
|--------|---------------|----------------|------------|
| Avg Query Time | 1.77s | 9.23s | 5.2x slower |
| Min Query Time | 1.68s | 8.30s | 4.9x slower |
| Max Query Time | 1.87s | 10.20s | 5.5x slower |
| Setup Complexity | Higher | Lower | N/A |
| Maintenance | Required | Not Required | N/A |

## Technical Implementation

### Hybrid Tables Approach
The benchmark follows these steps:
1. Creates a hybrid table for watermarks
2. Identifies all unique company IDs in the business table
3. Generates watermark records with a standard date for each company
4. Executes and times JOIN queries that filter records newer than their watermarks
5. Reports statistics on query performance

### Temporary Table Approach
The alternative implementation:
1. Fetches the list of company IDs from the business table
2. For each iteration:
   - Creates a temporary table
   - Inserts company IDs in batches
   - Creates a CTE that joins with the business table
   - Applies the same timestamp filter criteria as the hybrid table approach
   - Drops the temporary table
3. Reports performance statistics

## Conclusion

The benchmark results show a significant performance advantage for the hybrid table approach. Querying against the hybrid table was over 5 times faster than the temporary table approach, with average query times of 1.77 seconds versus 9.23 seconds.

While the temporary table approach eliminates the need for maintaining a persistent watermarks table, this comes at a considerable performance cost. The overhead of creating a temporary table, inserting batches of company IDs, and then dropping the table for each query significantly impacts performance.

The hybrid table approach proves to be substantially more efficient for scenarios involving repeated watermark-based queries, which is common in data pipeline processing. The initial setup cost of creating a hybrid table is quickly offset by the performance gains in query execution.

## Considerations

### Hybrid Tables Approach
- **Pros**: 
  - Much faster query performance (5.2x in our tests)
  - Persistent storage of watermarks
  - Supports complex watermark scenarios (multiple pipelines, different timestamp fields)
  - Can be updated incrementally
- **Cons**:
  - Requires table creation and maintenance
  - Additional storage costs

### Temporary Table Approach
- **Pros**:
  - No need to create or maintain a separate persistent table
  - Simpler implementation for one-off jobs
  - Reduced administration overhead
- **Cons**:
  - Significantly slower performance (5.2x in our tests)
  - Higher compute costs due to longer execution times
  - Repeated overhead of creating temporary tables
  - Potentially inefficient for repeated query patterns

## Next Steps

Potential areas for further exploration:
- Compare performance with different watermark dates
- Test with various warehouse sizes to measure scalability
- Evaluate performance under concurrent access scenarios
- Compare against other approaches like materialized views or streams
- Test different sizes of company lists to find performance inflection points
- Investigate hybrid approaches (e.g., using temporary tables with pre-materialized lists)
- Explore optimization techniques for the temporary table approach 