# Snowflake Hybrid Tables Join Benchmark

## Overview

This benchmark tests the performance of joining a standard Snowflake table with a hybrid table using watermarks to filter data based on timestamps. The use case simulates a data pipeline that processes new records since the last run, tracking progress using watermark dates.

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

## Results

We ran 10 iterations of the join query and found:

- **Data Volume**: 118,288,140 matching records processed in the join
- **Average Query Time**: 1.83 seconds
- **Min Query Time**: 1.65 seconds
- **Max Query Time**: 2.22 seconds

## Technical Implementation

The benchmark follows these steps:
1. Creates a hybrid table for watermarks
2. Identifies all unique company IDs in the business table
3. Generates watermark records with a standard date for each company
4. Executes and times JOIN queries that filter records newer than their watermarks
5. Reports statistics on query performance

## Conclusion

Snowflake hybrid tables perform efficiently for watermark-based filtering scenarios. The ability to process over 118 million records in under 2 seconds demonstrates that hybrid tables can be effectively used in data pipeline scenarios where tracking the last processed datetime per partition is required.

The benchmark shows that using hybrid tables for maintaining watermarks is a viable approach for implementing incremental data processing patterns, offering good performance for large-scale data operations.

## Next Steps

Potential areas for further exploration:
- Compare performance with different watermark dates
- Test with various warehouse sizes to measure scalability
- Evaluate performance under concurrent access scenarios
- Compare against other approaches like materialized views or streams 