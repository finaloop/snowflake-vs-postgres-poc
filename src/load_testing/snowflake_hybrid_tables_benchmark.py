import time
import json
import random
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from threading import Lock
from snowflake.connector import connect

from .data_generator import generate_fake_record, generate_random_date


class SnowflakeHybridTablesBenchmark:
    def __init__(
        self,
        connection_name: str = "hybrid_table_poc",
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
    ):
        """
        Initialize the Snowflake hybrid tables benchmark using a named connection.

        Args:
            connection_name: Name of the Snowflake connection configuration
            warehouse: Optional override for warehouse
            database: Optional override for database
            schema: Optional override for schema
        """
        self.connection_name = connection_name
        self.warehouse_override = warehouse
        self.database_override = database or "HYBRID_TABLE_POC"
        self.schema_override = schema or "PUBLIC"
        self.business_table_name = "BUSINESS_DATA"
        self.watermarks_table_name = "WATERMARKS"
        self._connection = None
        self._connection_lock = Lock()

    @property
    def connection(self):
        """Get or create a shared Snowflake connection."""
        if self._connection is None:
            with self._connection_lock:
                if self._connection is None:
                    print("Creating shared Snowflake connection...")
                    self._connection = connect(connection_name=self.connection_name)

                    # Apply overrides if provided
                    cursor = self._connection.cursor()
                    try:
                        if self.warehouse_override:
                            cursor.execute(f"USE WAREHOUSE {self.warehouse_override}")
                        if self.database_override:
                            cursor.execute(f"USE DATABASE {self.database_override}")
                        if self.schema_override:
                            cursor.execute(f"USE SCHEMA {self.schema_override}")
                    finally:
                        cursor.close()
        return self._connection

    def setup_tables(self):
        """Create the business and watermarks tables if they don't exist."""
        cursor = self.connection.cursor()
        try:
            # Drop tables if they exist
            cursor.execute(f"DROP TABLE IF EXISTS {self.business_table_name}")
            cursor.execute(f"DROP TABLE IF EXISTS {self.watermarks_table_name}")

            # Create business table (standard table)
            cursor.execute(f"""
            CREATE OR REPLACE TABLE {self.business_table_name} (
                ID STRING PRIMARY KEY,
                PARTITION_KEY STRING NOT NULL,
                TRANSACTION_DATE TIMESTAMP_NTZ NOT NULL,
                AMOUNT DECIMAL(10, 2) NOT NULL,
                STATUS VARCHAR(50) NOT NULL,
                PAYLOAD VARIANT NOT NULL
            )
            """)
            
            # Create watermarks table (hybrid table)
            cursor.execute(f"""
            CREATE OR REPLACE HYBRID TABLE {self.watermarks_table_name} (
                COMPANY_ID STRING NOT NULL PRIMARY KEY,
                LAST_PROCESSED_DATE TIMESTAMP_NTZ NOT NULL,
                UPDATED_AT TIMESTAMP_NTZ NOT NULL
            )
            """)
            
            print(f"Created business table '{self.business_table_name}' and watermarks hybrid table '{self.watermarks_table_name}'")
        finally:
            cursor.close()

    def generate_business_data(self, num_records: int, num_partitions: int) -> None:
        """Generate and insert business data with partitions."""
        print(f"Generating {num_records} business records across {num_partitions} partitions...")
        
        partitions = [f"partition_{i}" for i in range(1, num_partitions + 1)]
        
        cursor = self.connection.cursor()
        try:
            # Insert in batches for better performance
            batch_size = min(1000, num_records)
            for batch_start in range(0, num_records, batch_size):
                batch_end = min(batch_start + batch_size, num_records)
                batch_count = batch_end - batch_start
                
                # Generate values for the entire batch
                values_list = []
                
                for _ in range(batch_count):
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
                    
                    # Format values for Snowflake
                    values = (
                        f"'{record['id']}',"
                        f"'{partition}',"
                        f"TO_TIMESTAMP('{record['transaction_date']}'),"
                        f"{record['amount']},"
                        f"'{record['status']}',"
                        f"PARSE_JSON('{json.dumps(payload)}')"
                    )
                    values_list.append(f"({values})")
                
                # Execute batch insert
                values_sql = ",\n".join(values_list)
                insert_sql = f"""
                INSERT INTO {self.business_table_name} (
                    ID, PARTITION_KEY, TRANSACTION_DATE, AMOUNT, STATUS, PAYLOAD
                ) VALUES 
                {values_sql}
                """
                
                cursor.execute(insert_sql)
                
                print(f"Inserted batch of {batch_count} records ({batch_end}/{num_records})")
                
        finally:
            cursor.close()
    
    def generate_watermarks(self, num_partitions: int) -> None:
        """Generate and insert watermark data."""
        print(f"Generating watermarks for {num_partitions} partitions...")
        
        partitions = [f"partition_{i}" for i in range(1, num_partitions + 1)]
        
        cursor = self.connection.cursor()
        try:
            # Generate watermarks for all partitions
            values_list = []
            
            for partition in partitions:
                # Random date from the past few months
                last_processed_date = generate_random_date(
                    start_date=datetime.now() - timedelta(days=90),
                    end_date=datetime.now() - timedelta(days=1)
                )
                updated_at = last_processed_date + timedelta(minutes=random.randint(1, 60))
                
                values = (
                    f"'{partition}',"
                    f"TO_TIMESTAMP('{last_processed_date}'),"
                    f"TO_TIMESTAMP('{updated_at}')"
                )
                values_list.append(f"({values})")
            
            # Execute batch insert
            values_sql = ",\n".join(values_list)
            insert_sql = f"""
            INSERT INTO {self.watermarks_table_name} (
                COMPANY_ID, LAST_PROCESSED_DATE, UPDATED_AT
            ) VALUES 
            {values_sql}
            """
            
            cursor.execute(insert_sql)
            
            print(f"Inserted {len(values_list)} watermark records")
                
        finally:
            cursor.close()

    def run_direct_query_benchmark(self, num_iterations: int = 10) -> Dict[str, Any]:
        """Run benchmark for direct querying on Snowflake."""
        print(f"Running direct query benchmark on Snowflake for {num_iterations} iterations...")
        
        cursor = self.connection.cursor()
        try:
            # Run benchmark
            query_times = []
            record_counts = []
            
            for i in range(num_iterations):
                # Measure query time
                start_time = time.time()
                
                # Run query that joins watermarks with business data
                cursor.execute(f"""
                SELECT COUNT(*)
                FROM {self.business_table_name} b
                JOIN {self.watermarks_table_name} w
                  ON b.PARTITION_KEY = w.COMPANY_ID
                WHERE b.TRANSACTION_DATE > w.LAST_PROCESSED_DATE
                """)
                
                results = cursor.fetchone()[0]
                query_time = time.time() - start_time
                
                query_times.append(query_time)
                record_counts.append(results)
                
                print(f"Iteration {i+1}/{num_iterations}: Retrieved {results} records in {query_time:.4f} seconds")
            
            # Calculate statistics
            avg_query_time = sum(query_times) / len(query_times)
            avg_record_count = sum(record_counts) / len(record_counts)
            
            return {
                "database": "Snowflake Direct",
                "iterations": num_iterations,
                "avg_query_time_seconds": avg_query_time,
                "avg_records_retrieved": avg_record_count,
                "min_query_time": min(query_times),
                "max_query_time": max(query_times),
                "total_time_seconds": sum(query_times)
            }
                
        finally:
            cursor.close()

    def run_benchmark(self, 
                     num_business_records: int = 10000,
                     num_partitions: int = 10,
                     num_iterations: int = 10) -> Dict[str, Any]:
        """Run the full benchmark for Snowflake hybrid tables with watermarks."""
        print("Setting up Snowflake tables for watermarks benchmark...")
        self.setup_tables()
        
        print("Generating test data...")
        self.generate_business_data(num_business_records, num_partitions)
        self.generate_watermarks(num_partitions)
        
        print("Starting Snowflake direct query benchmark...")
        start_time = time.time()
        
        try:
            # Run the benchmark
            results = self.run_direct_query_benchmark(num_iterations)
            
            # Add total benchmark time
            total_time = time.time() - start_time
            results["total_benchmark_time_seconds"] = total_time
            
            return results
        finally:
            # Close connection
            if self._connection is not None:
                self._connection.close()
                self._connection = None 