import asyncio
import argparse
import json
import os
from typing import Dict, List, Any

from .snowflake_hybrid_tables_benchmark import SnowflakeHybridTablesBenchmark
from .postgres_hybrid_tables_benchmark import PostgresHybridTablesBenchmark


async def run_benchmark(
    benchmark_type: str,
    num_business_records: int = 10000,
    num_partitions: int = 10,
    num_pipelines: int = 5,
    num_iterations: int = 10,
    postgres_host: str = "localhost",
    postgres_port: int = 5432,
    postgres_user: str = "postgres",
    postgres_password: str = "postgres",
    postgres_database: str = "postgres",
    snowflake_connection: str = "hybrid_table_poc",
    snowflake_warehouse: str = None,
    snowflake_database: str = None,
    snowflake_schema: str = None,
) -> Dict[str, Any]:
    """Run the specified benchmark."""
    results = {}
    
    if benchmark_type == "snowflake" or benchmark_type == "both":
        print("\n=== Running Snowflake Hybrid Tables Benchmark ===\n")
        snowflake_benchmark = SnowflakeHybridTablesBenchmark(
            connection_name=snowflake_connection,
            warehouse=snowflake_warehouse,
            database=snowflake_database,
            schema=snowflake_schema
        )
        
        snowflake_results = snowflake_benchmark.run_benchmark(
            num_iterations=num_iterations
        )
        
        results["snowflake"] = snowflake_results
        print("\nSnowflake Benchmark Results:")
        print(json.dumps(snowflake_results, indent=2))
    
    if benchmark_type == "postgres" or benchmark_type == "both":
        print("\n=== Running PostgreSQL Benchmark ===\n")
        postgres_benchmark = PostgresHybridTablesBenchmark(
            host=postgres_host,
            port=postgres_port,
            user=postgres_user,
            password=postgres_password,
            database=postgres_database
        )
        
        postgres_results = await postgres_benchmark.run_benchmark(
            num_business_records=num_business_records,
            num_partitions=num_partitions,
            num_pipelines=num_pipelines,
            num_iterations=num_iterations
        )
        
        results["postgres"] = postgres_results
        print("\nPostgreSQL Benchmark Results:")
        print(json.dumps(postgres_results, indent=2))
    
    return results


async def main_async():
    """Parse command line arguments and run benchmarks asynchronously."""
    parser = argparse.ArgumentParser(description="Benchmark Snowflake Hybrid Tables vs PostgreSQL")
    
    # General benchmark settings
    parser.add_argument("--num-business-records", type=int, default=10000,
                       help="Number of business records to generate for PostgreSQL")
    parser.add_argument("--num-partitions", type=int, default=10,
                       help="Number of partitions to use for PostgreSQL")
    parser.add_argument("--num-pipelines", type=int, default=5,
                       help="Number of pipelines to simulate for PostgreSQL")
    parser.add_argument("--num-iterations", type=int, default=10,
                       help="Number of benchmark iterations to run")
    parser.add_argument("--output", type=str, default="hybrid_tables_benchmark_results.json",
                       help="Output file for benchmark results")
    
    # Database selection
    parser.add_argument("--benchmark", type=str, choices=["postgres", "snowflake", "both"], default="both",
                       help="Which benchmark to run")
    
    # PostgreSQL connection settings
    parser.add_argument("--postgres-host", type=str, default="localhost",
                       help="PostgreSQL host")
    parser.add_argument("--postgres-port", type=int, default=5432,
                       help="PostgreSQL port")
    parser.add_argument("--postgres-user", type=str, default="postgres",
                       help="PostgreSQL username")
    parser.add_argument("--postgres-password", type=str, default="postgres",
                       help="PostgreSQL password")
    parser.add_argument("--postgres-database", type=str, default="postgres",
                       help="PostgreSQL database name")
    
    # Snowflake connection settings
    parser.add_argument("--snowflake-connection", type=str, default="hybrid_table_poc",
                       help="Snowflake connection name from configuration")
    parser.add_argument("--snowflake-warehouse", type=str,
                       help="Snowflake warehouse (optional override)")
    parser.add_argument("--snowflake-database", type=str,
                       help="Snowflake database (optional override)")
    parser.add_argument("--snowflake-schema", type=str,
                       help="Snowflake schema (optional override)")
    
    args = parser.parse_args()
    
    # Run the benchmark
    results = await run_benchmark(
        benchmark_type=args.benchmark,
        num_business_records=args.num_business_records,
        num_partitions=args.num_partitions,
        num_pipelines=args.num_pipelines,
        num_iterations=args.num_iterations,
        postgres_host=args.postgres_host,
        postgres_port=args.postgres_port,
        postgres_user=args.postgres_user,
        postgres_password=args.postgres_password,
        postgres_database=args.postgres_database,
        snowflake_connection=args.snowflake_connection,
        snowflake_warehouse=args.snowflake_warehouse,
        snowflake_database=args.snowflake_database,
        snowflake_schema=args.snowflake_schema
    )
    
    # Save results to file
    with open(args.output, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\nResults saved to {args.output}")
    
    # Generate comparison if both benchmarks were run
    if "snowflake" in results and "postgres" in results:
        snowflake_time = results["snowflake"]["avg_query_time_seconds"]
        postgres_time = results["postgres"]["avg_query_time_seconds"]
        
        print("\n=== Benchmark Comparison ===")
        print(f"Snowflake avg query time: {snowflake_time:.6f} seconds")
        print(f"PostgreSQL avg query time: {postgres_time:.6f} seconds")
        
        if snowflake_time < postgres_time:
            speedup = postgres_time / snowflake_time
            print(f"Snowflake is {speedup:.2f}x faster than PostgreSQL")
        else:
            speedup = snowflake_time / postgres_time
            print(f"PostgreSQL is {speedup:.2f}x faster than Snowflake")


def main():
    """Run the main async function."""
    asyncio.run(main_async())


if __name__ == "__main__":
    main() 