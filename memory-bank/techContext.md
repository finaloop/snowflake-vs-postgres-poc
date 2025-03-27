# Technical Context: Snowpark vs Postgres POC

## Technologies Used
1. Core Technologies
   - Python 3.11+
   - PostgreSQL
   - Snowflake
   - Snowpark

2. Key Dependencies
   - psycopg[pool] >= 3.2.6
   - snowflake-connector-python == 3.13.0
   - hatchling (build system)

3. Development Tools
   - uv (Python package manager)
   - hatch (build system)
   - Python virtual environment

## Development Setup
1. Environment Requirements
   - Python 3.11 or higher
   - PostgreSQL instance
   - Snowflake account
   - Virtual environment

2. Installation Steps
   ```bash
   # Create virtual environment
   python -m venv .venv
   source .venv/bin/activate

   # Install dependencies
   uv pip install -e .
   ```

3. Configuration
   - Database connection strings
   - Snowflake credentials
   - Environment variables

## Technical Constraints
1. Python Version
   - Minimum: Python 3.11
   - Type hints required
   - Modern Python features used

2. Database Requirements
   - PostgreSQL: Recent version
   - Snowflake: Compatible with Snowpark
   - Sufficient storage and compute

3. Performance Considerations
   - Network latency
   - Database resources
   - Memory usage

## Dependencies
1. Core Dependencies
   - Database connectors
   - Data generation libraries
   - Analysis tools

2. Development Dependencies
   - Build tools
   - Testing frameworks
   - Documentation generators

3. Runtime Dependencies
   - Python standard library
   - Third-party packages
   - System libraries

## Development Workflow
1. Code Organization
   - src/snowpark_vs_postgres_poc/
   - Modular structure
   - Clear separation of concerns

2. Build Process
   - hatch build system
   - Package management with uv
   - Dependency locking

3. Testing Strategy
   - Unit tests
   - Integration tests
   - Performance tests 