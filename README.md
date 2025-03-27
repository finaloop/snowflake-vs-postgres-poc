# Snowpark vs Postgres POC

This repository contains benchmarks for comparing performance between Snowflake and PostgreSQL for various use cases.

## Benchmarks

### 1. Load Testing

Measures and compares the data insertion performance between:
- Snowflake Hybrid Tables
- PostgreSQL

To run:
```bash
python run_benchmark.py
```

### 2. Join Benchmark

Measures the performance of joins between Snowflake's standard tables and hybrid tables in a data pipeline watermark scenario.

To run:
```bash
python run_join_benchmark.py --benchmark snowflake
```

See the [detailed results](src/join_benchmark/join_benchmark.md) for more information about the join benchmark.

## Requirements

- Python 3.11+
- Snowflake account configured with `hybrid_table_poc` connection
- PostgreSQL database (optional, for load testing)

## Installation

```bash
# Create a virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -e .
```

Alternatively, use uv:
```bash
uv venv .venv
source .venv/bin/activate
uv pip install -e .
```
