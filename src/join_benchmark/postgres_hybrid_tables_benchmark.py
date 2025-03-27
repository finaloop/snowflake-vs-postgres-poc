import time
import json
import random
from datetime import datetime, timedelta
from typing import Dict, Any
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from ..load_testing.data_generator import generate_fake_record, generate_random_date


class PostgresHybridTablesBenchmark:
    def __init__(self, 
                 host: str = "localhost", 
                 port: int = 5432, 
                 user: str = "postgres", 
                 password: str = "postgres", 
                 database: str = "postgres"):
        self.connection_params = {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "dbname": database,
        }
        self.business_table_name = "business_data"
        self.watermarks_table_name = "watermarks"
        self._pool = None

    def create_connection(self):
        """Create a new synchronous database connection for setup and count operations."""
        return psycopg.connect(**self.connection_params, row_factory=dict_row)
    
    async def get_connection_pool(self, max_size: int = 20) -> AsyncConnectionPool:
        """Get or create an async connection pool."""
        if self._pool is None:
            conn_string = " ".join(f"{k}={v}" for k, v in self.connection_params.items())
            self._pool = AsyncConnectionPool(conn_string, min_size=5, max_size=max_size)
            # Wait for the pool to be ready
            await self._pool.wait()
        return self._pool

    def setup_tables(self):
        """Create the business and watermarks tables if they don't exist."""
        with self.create_connection() as conn:
            with conn.cursor() as cur:
                # Drop tables if they exist
                cur.execute(f"DROP TABLE IF EXISTS {self.business_table_name}")
                cur.execute(f"DROP TABLE IF EXISTS {self.watermarks_table_name}")
                
                # Create business table
                cur.execute(f"""
                CREATE TABLE {self.business_table_name} (
                    id UUID PRIMARY KEY,
                    partition_key TEXT NOT NULL,
                    transaction_date TIMESTAMP NOT NULL,
                    amount DECIMAL(10, 2) NOT NULL,
                    status VARCHAR(50) NOT NULL,
                    payload JSONB NOT NULL
                )
                """)
                
                # Create watermarks table
                cur.execute(f"""
                CREATE TABLE {self.watermarks_table_name} (
                    partition_key TEXT NOT NULL,
                    last_processed_date TIMESTAMP NOT NULL,
                    pipeline_id TEXT NOT NULL,
                    updated_at TIMESTAMP NOT NULL,
                    PRIMARY KEY (partition_key, pipeline_id)
                )
                """)
                
                conn.commit()
                print(f"Created business table '{self.business_table_name}' and watermarks table '{self.watermarks_table_name}'")

    async def generate_business_data(self, num_records: int, num_partitions: int) -> None:
        """Generate and insert business data with partitions."""
        print(f"Generating {num_records} business records across {num_partitions} partitions...")
        
        partitions = [f"partition_{i}" for i in range(1, num_partitions + 1)]
        
        pool = await self.get_connection_pool()
        
        # Insert in batches for better performance
        batch_size = min(1000, num_records)
        for batch_start in range(0, num_records, batch_size):
            batch_end = min(batch_start + batch_size, num_records)
            batch_count = batch_end - batch_start
            
            # Prepare batch insert
            values_list = []
            params = []
            
            for i in range(batch_count):
                record = generate_fake_record()
                partition = random.choice(partitions)
                
                # Payload as JSON
                payload = {
                    "user_id": record["user_id"],
                    "product_id": record["product_id"],
                    "customer_name": record["customer_name"],
                    "email": record["email"],
                    "shipping_address": record["shipping_address"],
                    "payment_method": record["payment_method"],
                    "metadata": record["metadata"]
                }
                
                # Add parameters
                params.extend([
                    record["id"],
                    partition,
                    record["transaction_date"],
                    record["amount"],
                    record["status"],
                    json.dumps(payload)
                ])
                
                param_idx = i * 6  # 6 parameters per record
                values_list.append(
                    "($%s, $%s, $%s, $%s, $%s, $%s)" % (
                        param_idx + 1, param_idx + 2, param_idx + 3,
                        param_idx + 4, param_idx + 5, param_idx + 6
                    )
                )
            
            # Execute batch insert
            values_sql = ",\n".join(values_list)
            insert_sql = f"""
            INSERT INTO {self.business_table_name} (
                id, partition_key, transaction_date, amount, status, payload
            ) VALUES 
            {values_sql}
            """
            
            async with pool.connection() as aconn:
                await aconn.execute(insert_sql, params)
            
            print(f"Inserted batch of {batch_count} records ({batch_end}/{num_records})")

    async def generate_watermarks(self, num_partitions: int, num_pipelines: int) -> None:
        """Generate and insert watermark data."""
        print(f"Generating watermarks for {num_partitions} partitions and {num_pipelines} pipelines...")
        
        partitions = [f"partition_{i}" for i in range(1, num_partitions + 1)]
        pipelines = [f"pipeline_{i}" for i in range(1, num_pipelines + 1)]
        
        pool = await self.get_connection_pool()
        
        # Generate watermarks for all partition-pipeline combinations
        values_list = []
        params = []
        
        count = 0
        for partition in partitions:
            for pipeline in pipelines:
                # Random date from the past few months
                last_processed_date = generate_random_date(
                    start_date=datetime.now() - timedelta(days=90),
                    end_date=datetime.now() - timedelta(days=1)
                )
                updated_at = last_processed_date + timedelta(minutes=random.randint(1, 60))
                
                # Add parameters
                params.extend([
                    partition,
                    last_processed_date,
                    pipeline,
                    updated_at
                ])
                
                param_idx = count * 4  # 4 parameters per record
                values_list.append(
                    "($%s, $%s, $%s, $%s)" % (
                        param_idx + 1, param_idx + 2, param_idx + 3, param_idx + 4
                    )
                )
                count += 1
        
        # Execute batch insert
        values_sql = ",\n".join(values_list)
        insert_sql = f"""
        INSERT INTO {self.watermarks_table_name} (
            partition_key, last_processed_date, pipeline_id, updated_at
        ) VALUES 
        {values_sql}
        """
        
        async with pool.connection() as aconn:
            await aconn.execute(insert_sql, params)
        
        print(f"Inserted {len(values_list)} watermark records")

    async def run_direct_query_benchmark(self, num_iterations: int = 10) -> Dict[str, Any]:
        """Run benchmark for direct querying on Postgres."""
        print(f"Running direct query benchmark on Postgres for {num_iterations} iterations...")
        
        # Get all partitions and pipelines for random selection
        with self.create_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT DISTINCT partition_key FROM {self.watermarks_table_name}")
                partitions = [row["partition_key"] for row in cur.fetchall()]
                
                cur.execute(f"SELECT DISTINCT pipeline_id FROM {self.watermarks_table_name}")
                pipelines = [row["pipeline_id"] for row in cur.fetchall()]
        
        if not partitions or not pipelines:
            raise ValueError("No partitions or pipelines found in watermarks table.")
        
        # Run benchmark
        pool = await self.get_connection_pool()
        query_times = []
        record_counts = []
        
        for i in range(num_iterations):
            # Select random partition and pipeline
            partition = random.choice(partitions)
            pipeline = random.choice(pipelines)
            
            # Measure query time
            start_time = time.time()
            
            async with pool.connection() as aconn:
                # Run query that joins watermarks with business data
                async with aconn.cursor(row_factory=dict_row) as acur:
                    await acur.execute(f"""
                    SELECT b.*
                    FROM {self.business_table_name} b
                    JOIN {self.watermarks_table_name} w
                      ON b.partition_key = w.partition_key
                    WHERE b.partition_key = %s
                      AND w.pipeline_id = %s
                      AND b.transaction_date > w.last_processed_date
                    ORDER BY b.transaction_date
                    """, (partition, pipeline))
                    
                    results = await acur.fetchall()
            
            query_time = time.time() - start_time
            
            query_times.append(query_time)
            record_counts.append(len(results))
            
            print(f"Iteration {i+1}/{num_iterations}: Retrieved {len(results)} records in {query_time:.4f} seconds")
        
        # Calculate statistics
        avg_query_time = sum(query_times) / len(query_times)
        avg_record_count = sum(record_counts) / len(record_counts)
        
        return {
            "database": "PostgreSQL Direct",
            "iterations": num_iterations,
            "avg_query_time_seconds": avg_query_time,
            "avg_records_retrieved": avg_record_count,
            "min_query_time": min(query_times),
            "max_query_time": max(query_times),
            "total_time_seconds": sum(query_times)
        }

    async def run_benchmark(self, 
                     num_business_records: int = 10000,
                     num_partitions: int = 10,
                     num_pipelines: int = 5,
                     num_iterations: int = 10) -> Dict[str, Any]:
        """Run the full benchmark for Postgres tables with watermarks."""
        print("Setting up Postgres tables for watermarks benchmark...")
        self.setup_tables()
        
        print("Generating test data...")
        await self.generate_business_data(num_business_records, num_partitions)
        await self.generate_watermarks(num_partitions, num_pipelines)
        
        print("Starting Postgres direct query benchmark...")
        start_time = time.time()
        
        try:
            # Run the benchmark
            results = await self.run_direct_query_benchmark(num_iterations)
            
            # Add total benchmark time
            total_time = time.time() - start_time
            results["total_benchmark_time_seconds"] = total_time
            
            return results
        finally:
            # Close connection pool
            if self._pool is not None:
                await self._pool.close()
                self._pool = None 