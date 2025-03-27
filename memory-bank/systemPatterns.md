# System Patterns: Snowpark vs Postgres POC

## System Architecture
1. Core Components
   - Data Generator: Creates synthetic test data
   - Benchmark Executor: Runs tests on both systems
   - Results Analyzer: Processes and compares results
   - Report Generator: Creates documentation and insights
   - Hybrid Tables Benchmark: Tests Snowflake join patterns

2. Component Relationships
   ```mermaid
   graph TD
       DG[Data Generator] --> BE[Benchmark Executor]
       BE --> RA[Results Analyzer]
       RA --> RG[Report Generator]
       BE --> SF[Snowflake]
       BE --> PG[PostgreSQL]
       HTB[Hybrid Tables Benchmark] --> SF
   ```

## Key Technical Decisions
1. Data Generation
   - Python-based synthetic data generation
   - Configurable data volumes and types
   - Consistent data across systems

2. Benchmark Execution
   - Separate modules for each database
   - Identical query execution
   - Precise timing measurements

3. Results Processing
   - JSON-based results storage
   - Statistical analysis of metrics
   - Markdown report generation

4. Snowflake Watermark Patterns
   - Hybrid tables for persistent watermarks
   - Temporary tables for dynamic watermarks
   - Performance comparison between approaches

## Design Patterns in Use
1. Strategy Pattern
   - Abstract database interfaces
   - Pluggable database implementations
   - Consistent benchmarking approach

2. Factory Pattern
   - Data generation factories
   - Query execution factories
   - Report generation factories

3. Observer Pattern
   - Progress monitoring
   - Results collection
   - Error handling

4. Benchmark Pattern
   - Setup/teardown for test environment
   - Multiple iterations for statistical validity
   - Consistent methodology across tests

## Component Relationships
1. Data Flow
   - Data Generator → Database Systems
   - Database Systems → Results Analyzer
   - Results Analyzer → Report Generator

2. Control Flow
   - Main orchestrator → Component coordination
   - Components → Error handling
   - Results → Analysis pipeline

3. Dependencies
   - Python 3.11+ runtime
   - Database connectors
   - Analysis libraries

## Error Handling Patterns
1. Database Errors
   - Connection retry logic
   - Query timeout handling
   - Resource cleanup

2. Data Generation Errors
   - Validation checks
   - Recovery mechanisms
   - Data consistency verification

3. Analysis Errors
   - Graceful degradation
   - Partial results handling
   - Error reporting

## Snowflake Integration Patterns
1. Hybrid Tables Usage
   - Primary key for company-level data
   - Timestamp watermarks for incremental processing
   - Efficient joins with large tables

2. Temporary Tables Approach
   - Dynamic creation of company lists
   - Batch processing for large datasets
   - Clean up after query execution

3. Connection Management
   - Named connections via connections.toml
   - External browser authentication
   - Context management for resources 