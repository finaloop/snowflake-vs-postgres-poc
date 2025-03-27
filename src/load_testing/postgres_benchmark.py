import time
import json
import asyncio
from typing import Dict, Any, Tuple
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from .data_generator import generate_fake_record


class PostgresBenchmark:
    def __init__(self, 
                 host: str = "db-data-platform-core.cae1akbgjeh3.us-east-1.rds.amazonaws.com", 
                 port: int = 5432, 
                 user: str = "admin_integ", 
                 password: str = "tDabYnQRrJerqQJ0arzi", 
                 database: str = "data-platform-core-integ"):
        self.connection_params = {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "dbname": database,
        }
        self.table_name = "transactions"
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

    def setup_table(self):
        """Create the test table if it doesn't exist."""
        with self.create_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                DROP TABLE IF EXISTS {self.table_name};
                
                CREATE TABLE {self.table_name} (
                    id UUID PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    product_id INTEGER NOT NULL,
                    transaction_date TIMESTAMP NOT NULL,
                    amount DECIMAL(10, 2) NOT NULL,
                    status VARCHAR(50) NOT NULL,
                    payment_method VARCHAR(50) NOT NULL,
                    customer_name VARCHAR(100) NOT NULL,
                    email VARCHAR(100) NOT NULL,
                    shipping_address TEXT NOT NULL,
                    metadata JSONB NOT NULL
                )
                """)
                conn.commit()

    async def insert_single_row(self, record: Dict[str, Any], pool: AsyncConnectionPool) -> Tuple[int, float]:
        """Insert a single record into the PostgreSQL table."""
        start_time = time.time()
        
        # Convert record to row for insertion
        row = (
            record["id"],
            record["user_id"],
            record["product_id"],
            record["transaction_date"],
            record["amount"],
            record["status"],
            record["payment_method"],
            record["customer_name"],
            record["email"],
            record["shipping_address"],
            json.dumps(record["metadata"])
        )
        
        async with pool.connection() as aconn:
            # Execute single row insert
            await aconn.execute(
                f"""
                INSERT INTO {self.table_name} (
                    id, user_id, product_id, transaction_date, amount,
                    status, payment_method, customer_name, email,
                    shipping_address, metadata
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                row
            )
            
        end_time = time.time()
        return 1, end_time - start_time

    async def run_concurrent_inserts(self, 
                                    inserts_per_second: int = 3000, 
                                    total_records: int = 100000) -> Tuple[int, float]:
        """Run concurrent inserts to achieve the desired insert rate."""
        concurrent_operations = min(inserts_per_second, 100)  # Cap at 100 concurrent operations instead of 20
        
        # Ensure we have at least 1 concurrent operation
        concurrent_operations = max(concurrent_operations, 1)
        
        total_inserted = 0
        total_time = 0
        
        print(f"PostgreSQL: Running {total_records} single row inserts with {concurrent_operations} concurrent operations")
        
        # Async implementation using semaphore
        pool = await self.get_connection_pool(concurrent_operations)
        sem = asyncio.Semaphore(concurrent_operations)
        
        async def process_item(item_index):
            nonlocal total_inserted, total_time
            async with sem:
                record = generate_fake_record()
                count, operation_time = await self.insert_single_row(record, pool)
                return count, operation_time
        
        batch_size = concurrent_operations * 2
        
        for i in range(0, total_records, batch_size):
            start_time = time.time()
            
            # Create and gather tasks for this batch
            tasks = []
            batch_count = min(batch_size, total_records - i)
            for j in range(batch_count):
                tasks.append(asyncio.create_task(process_item(i + j)))
            
            # Wait for all operations to complete
            results = await asyncio.gather(*tasks)
            
            # Process results
            for count, operation_time in results:
                total_inserted += count
                total_time += operation_time
            
            # Calculate sleep time to maintain the desired insertion rate
            elapsed = time.time() - start_time
            target_time = batch_count / inserts_per_second
            if elapsed < target_time:
                await asyncio.sleep(target_time - elapsed)
            
            # Print progress
            current_rate = total_inserted / max(total_time, 0.001)
            print(f"PostgreSQL progress: {total_inserted}/{total_records} records inserted "
                  f"({total_inserted/total_records*100:.1f}%), " 
                  f"Current rate: {current_rate:.2f} records/second")
        
        # Close the pool
        await pool.close()
        
        return total_inserted, total_time
    
    def count_records(self) -> int:
        """Count the number of records in the table."""
        with self.create_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) as count FROM {self.table_name}")
                result = cur.fetchone()
                return result["count"]
                
    async def run_benchmark(self, 
                     inserts_per_second: int = 3000, 
                     total_records: int = 100000) -> Dict[str, Any]:
        """Run the full PostgreSQL benchmark."""
        print("Setting up PostgreSQL table...")
        self.setup_table()
        
        print(f"Starting PostgreSQL benchmark with target: {inserts_per_second} inserts/second")
        start_time = time.time()
        
        # Run the concurrent inserts asynchronously
        total_inserted, insert_time = await self.run_concurrent_inserts(
            inserts_per_second=inserts_per_second, 
            total_records=total_records
        )
        
        total_time = time.time() - start_time
        
        # Count records and collect results
        actual_count = self.count_records()
        
        return {
            "database": "PostgreSQL",
            "target_inserts_per_second": inserts_per_second,
            "total_records": total_records,
            "actual_records_inserted": actual_count,
            "total_time_seconds": total_time,
            "insert_time_seconds": insert_time,
            "average_inserts_per_second": actual_count / total_time if total_time > 0 else 0,
        } 