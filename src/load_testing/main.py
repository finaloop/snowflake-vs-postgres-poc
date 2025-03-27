import argparse
import json
import os
import time
from typing import Dict, Any, List
import asyncio

# Using local imports
from .postgres_benchmark import PostgresBenchmark
from .snowflake_benchmark import SnowflakeBenchmark


def format_result(result: Dict[str, Any]) -> str:
    """Format the benchmark result for display."""
    return (
        f"\n--- {result['database']} Benchmark Results ---\n"
        f"Total records: {result['actual_records_inserted']}/{result['total_records']}\n"
        f"Total time: {result['total_time_seconds']:.2f} seconds\n"
        f"Insert time: {result['insert_time_seconds']:.2f} seconds\n"
        f"Target insert rate: {result['target_inserts_per_second']} records/second\n"
        f"Actual average rate: {result['average_inserts_per_second']:.2f} records/second\n"
    )


def compare_results(results: List[Dict[str, Any]]) -> str:
    """Compare benchmark results between databases."""
    if len(results) < 2:
        return "Not enough results to compare."
    
    # Find the fastest database by average inserts per second
    fastest = max(results, key=lambda x: x["average_inserts_per_second"])
    
    comparison = "\n--- Performance Comparison ---\n"
    for result in results:
        if result == fastest:
            comparison += f"{result['database']}: {result['average_inserts_per_second']:.2f} records/second (Fastest)\n"
        else:
            pct_diff = (fastest["average_inserts_per_second"] / result["average_inserts_per_second"] - 1) * 100
            comparison += (f"{result['database']}: {result['average_inserts_per_second']:.2f} records/second "
                          f"({pct_diff:.1f}% slower than {fastest['database']})\n")
    
    return comparison


def save_results_to_file(results: List[Dict[str, Any]], filename: str = "benchmark_results.json"):
    """Save benchmark results to a JSON file."""
    # Add timestamp
    results_with_time = {
        "timestamp": time.time(),
        "results": results
    }
    
    with open(filename, "w") as f:
        json.dump(results_with_time, f, indent=2, default=str)
    
    print(f"Results saved to {filename}")


async def run_benchmarks(args):
    """Run benchmarks for PostgreSQL and Snowflake."""
    results = []
    
    # Run PostgreSQL benchmark if enabled
    if args.postgres:
        postgres_benchmark = PostgresBenchmark(
            host=args.postgres_host,
            port=args.postgres_port,
            user=args.postgres_user,
            password=args.postgres_password,
            database=args.postgres_database
        )
        
        postgres_result = await postgres_benchmark.run_benchmark(
            inserts_per_second=args.inserts_per_second,
            total_records=args.total_records
        )
        
        results.append(postgres_result)
        print(format_result(postgres_result))
    
    # Run Snowflake benchmark if enabled
    if args.snowflake:
        snowflake_benchmark = SnowflakeBenchmark(
            connection_name=args.snowflake_connection,
            warehouse=args.snowflake_warehouse,
            database=args.snowflake_database,
            schema=args.snowflake_schema
        )
        
        snowflake_result = await snowflake_benchmark.run_benchmark(
            inserts_per_second=args.inserts_per_second,
            total_records=args.total_records
        )
        
        results.append(snowflake_result)
        print(format_result(snowflake_result))
    
    # Display comparison if both benchmarks ran
    if len(results) > 1:
        print(compare_results(results))
    
    # Save results to file
    if args.output:
        save_results_to_file(results, args.output)
    
    return results


def main():
    """Parse command line arguments and run benchmarks."""
    parser = argparse.ArgumentParser(description="Benchmark Postgres vs Snowflake Hybrid Tables")
    
    # General benchmark settings
    parser.add_argument("--inserts-per-second", type=int, default=3000,
                       help="Target insert rate (records per second)")
    parser.add_argument("--total-records", type=int, default=10000,
                       help="Total number of records to insert")
    parser.add_argument("--output", type=str, default="benchmark_results.json",
                       help="Output file for benchmark results")
    
    # Database selection
    parser.add_argument("--postgres", action="store_true", help="Run PostgreSQL benchmark")
    parser.add_argument("--snowflake", action="store_true", help="Run Snowflake benchmark")
    
    # PostgreSQL connection settings
    parser.add_argument("--postgres-host", type=str, default="localhost",
                       help="PostgreSQL host")
    parser.add_argument("--postgres-port", type=int, default=5433,
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
    
    # Default to running both if neither is specified
    if not args.postgres and not args.snowflake:
        args.postgres = True
        args.snowflake = True
    
    # Run the benchmarks
    asyncio.run(run_benchmarks(args))


if __name__ == "__main__":
    main() 