# Snowpark vs Postgres POC

This repository contains benchmarks for comparing performance between Snowflake and PostgreSQL for various use cases.

## Benchmarks

### 1. Load Testing

Measures and compares the data insertion performance between:
- Snowflake Hybrid Tables
- PostgreSQL

To run:
```bash
uv run run_benchmark.py
```

### 2. Join Benchmark

Measures the performance of joins between Snowflake's standard tables and hybrid tables in a data pipeline watermark scenario.

To run:
```bash
uv run run_join_benchmark.py
```

See the [detailed results](src/join_benchmark/README.md) for more information about the join benchmark.

## Requirements

- Python 3.11+
- [UV](https://github.com/astral-sh/uv) package manager
- Snowflake account configured with `hybrid_table_poc` connection
- PostgreSQL database (optional, for load testing)

## Installation

```bash
# Install all dependencies using uv
uv sync

# Install additional packages as needed
uv add snowflake-connector-python
```

## Snowflake Configuration

Create a `~/.snowflake/connections.toml` file with your Snowflake connection details:

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
