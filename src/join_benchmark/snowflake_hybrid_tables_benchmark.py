import time
from datetime import datetime
from typing import Dict, List, Any, Optional
from threading import Lock
from snowflake.connector import connect



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
        self.database_override = database or "FINALOOP"
        self.schema_override = schema or "PUBLIC"
        self.business_table_name = "FINALOOP.MODELS.AOV_AOGP_HISTOGRAM"
        self.watermarks_table_name = f"{self.database_override}.{self.schema_override}.WATERMARKS"
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
                            
                        # Verify we have a working schema context
                        print(f"Using database: {self.database_override}, schema: {self.schema_override}")
                    finally:
                        cursor.close()
        return self._connection

    def setup_watermarks_table(self):
        """Create the watermarks table if it doesn't exist."""
        cursor = self.connection.cursor()
        try:
            # Drop watermarks table if it exists
            cursor.execute(f"DROP TABLE IF EXISTS {self.watermarks_table_name}")
            
            # Create watermarks table (hybrid table)
            cursor.execute(f"""
            CREATE OR REPLACE HYBRID TABLE {self.watermarks_table_name} (
                COMPANY_ID STRING NOT NULL PRIMARY KEY,
                LAST_PROCESSED_DATE TIMESTAMP_NTZ NOT NULL,
                UPDATED_AT TIMESTAMP_NTZ NOT NULL
            )
            """)
            
            print(f"Created watermarks hybrid table '{self.watermarks_table_name}'")
        finally:
            cursor.close()

    def get_company_ids(self) -> List[str]:
        """Get a list of all company_ids from the business table."""
        cursor = self.connection.cursor()
        try:
            cursor.execute(f"""
            SELECT DISTINCT COMPANY_ID 
            FROM {self.business_table_name}
            WHERE COMPANY_ID IS NOT NULL
            """)
            company_ids = [row[0] for row in cursor.fetchall()]
            print(f"Found {len(company_ids)} unique companies in the business table")
            return company_ids
        finally:
            cursor.close()
    
    def generate_watermarks(self) -> None:
        """Generate and insert watermark data with standard dates."""
        # Get company IDs from business table
        company_ids = self.get_company_ids()
        if not company_ids:
            raise ValueError("No company_ids found in business table")
        
        num_companies = len(company_ids)
        print(f"Generating watermarks for {num_companies} companies...")
        
        # Use a fixed date in the past to ensure we get data
        last_processed_date = datetime(2020, 1, 1)
        updated_at = datetime.now()
        
        cursor = self.connection.cursor()
        try:
            # Generate watermarks for all companies
            values_list = []
            
            for company_id in company_ids:
                values = (
                    f"'{company_id}',"
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
            
            print(f"Inserted {len(values_list)} watermark records with watermark date {last_processed_date}")
                
        finally:
            cursor.close()

    def run_direct_query_benchmark(self, num_iterations: int = 10) -> Dict[str, Any]:
        """Run benchmark for direct querying on Snowflake."""
        print(f"Running direct query benchmark on Snowflake for {num_iterations} iterations...")
        
        cursor = self.connection.cursor()
        try:
            # Run benchmark
            query_times = []
            
            for i in range(num_iterations):
                # Measure query time
                start_time = time.time()
                
                # Run query that joins watermarks with business data for ALL companies
                # Without LIMIT and without fetching results
                cursor.execute(f"""
                SELECT COUNT(*)
                FROM {self.business_table_name} b
                JOIN {self.watermarks_table_name} w
                  ON b.COMPANY_ID = w.COMPANY_ID
                WHERE b.ORDER_CREATED_DATE > w.LAST_PROCESSED_DATE
                """)
                
                # Just get the count without fetching all records
                count_result = cursor.fetchone()[0]
                query_time = time.time() - start_time
                
                query_times.append(query_time)
                
                print(f"Iteration {i+1}/{num_iterations}: Found {count_result} matching records in {query_time:.4f} seconds")
            
            # Calculate statistics
            avg_query_time = sum(query_times) / len(query_times)
            
            return {
                "database": "Snowflake Direct",
                "iterations": num_iterations,
                "avg_query_time_seconds": avg_query_time,
                "min_query_time": min(query_times),
                "max_query_time": max(query_times),
                "total_time_seconds": sum(query_times),
                "record_count": count_result
            }
                
        finally:
            cursor.close()

    def run_split_to_table_benchmark(self, num_iterations: int = 10) -> Dict[str, Any]:
        """Run benchmark using SPLIT_TO_TABLE approach instead of hybrid tables."""
        print(f"Running SPLIT_TO_TABLE benchmark for {num_iterations} iterations...")
        
        # Get company IDs
        company_ids = self.get_company_ids()
        if not company_ids:
            raise ValueError("No company_ids found in business table")
        
        # Fixed watermark date (same as in hybrid table benchmark)
        last_processed_date = datetime(2020, 1, 1)
        
        cursor = self.connection.cursor()
        try:
            # Run benchmark
            query_times = []
            
            for i in range(num_iterations):
                # Measure query time
                start_time = time.time()
                
                # Create a temporary table with company IDs
                temp_table_name = f"TEMP_COMPANY_IDS_{int(time.time())}"
                
                # Create temp table
                cursor.execute(f"""
                CREATE OR REPLACE TEMPORARY TABLE {temp_table_name} (
                    COMPANY_ID STRING
                )
                """)
                
                # Insert company IDs in batches to avoid query size limitations
                batch_size = 100
                for j in range(0, len(company_ids), batch_size):
                    batch = company_ids[j:j+batch_size]
                    values = ", ".join([f"('{company_id}')" for company_id in batch])
                    cursor.execute(f"""
                    INSERT INTO {temp_table_name} (COMPANY_ID)
                    VALUES {values}
                    """)
                
                # Run the actual benchmark query
                cursor.execute(f"""
                WITH company_watermarks AS (
                    SELECT 
                        t.COMPANY_ID,
                        TO_TIMESTAMP('{last_processed_date}') AS LAST_PROCESSED_DATE
                    FROM {temp_table_name} t
                )
                SELECT COUNT(*)
                FROM {self.business_table_name} b
                JOIN company_watermarks w
                  ON b.COMPANY_ID = w.COMPANY_ID
                WHERE b.ORDER_CREATED_DATE > w.LAST_PROCESSED_DATE
                """)
                
                # Just get the count without fetching all records
                count_result = cursor.fetchone()[0]
                query_time = time.time() - start_time
                
                query_times.append(query_time)
                
                print(f"Iteration {i+1}/{num_iterations}: Found {count_result} matching records in {query_time:.4f} seconds")
                
                # Clean up temp table
                cursor.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
            
            # Calculate statistics
            avg_query_time = sum(query_times) / len(query_times)
            
            return {
                "database": "Snowflake SPLIT_TO_TABLE",
                "iterations": num_iterations,
                "avg_query_time_seconds": avg_query_time,
                "min_query_time": min(query_times),
                "max_query_time": max(query_times),
                "total_time_seconds": sum(query_times),
                "record_count": count_result
            }
                
        finally:
            cursor.close()

    def run_benchmark(self, num_iterations: int = 10) -> Dict[str, List[Dict[str, Any]]]:
        """Run the full benchmark comparing hybrid tables vs SPLIT_TO_TABLE approach."""
        print("Setting up Snowflake watermarks table for benchmark...")
        self.setup_watermarks_table()
        
        print("Generating watermark data...")
        self.generate_watermarks()
        
        print("Starting Snowflake direct query benchmark...")
        start_time = time.time()
        
        try:
            # Run hybrid table benchmark
            hybrid_results = self.run_direct_query_benchmark(num_iterations)
            
            # Run SPLIT_TO_TABLE benchmark
            print("\nStarting SPLIT_TO_TABLE benchmark...")
            split_results = self.run_split_to_table_benchmark(num_iterations)
            
            # Add total benchmark time
            total_time = time.time() - start_time
            
            # Combine results
            return {
                "total_benchmark_time_seconds": total_time,
                "results": [hybrid_results, split_results]
            }
        finally:
            # Close connection
            if self._connection is not None:
                self._connection.close()
                self._connection = None 

if __name__ == "__main__":
    benchmark = SnowflakeHybridTablesBenchmark()
    results = benchmark.run_benchmark()
    print(results)