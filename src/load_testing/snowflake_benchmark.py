import time
import json
from typing import Dict, List, Any, Tuple, Optional
from threading import Lock
import uuid
from snowflake.connector import connect

from .data_generator import generate_fake_record


class SnowflakeBenchmark:
    def __init__(
        self,
        connection_name: str = "hybrid_table_poc",
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
    ):
        """
        Initialize the Snowflake benchmark using a named connection.

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
        self.table_name = "TRANSACTIONS"
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

    def setup_table(self):
        """Create the test hybrid table if it doesn't exist."""
        cursor = self.connection.cursor()
        try:
            # Drop table if it exists
            cursor.execute(f"DROP TABLE IF EXISTS {self.table_name}")

            # Create hybrid table
            cursor.execute(f"""
            CREATE OR REPLACE HYBRID TABLE {self.table_name} (
                ID STRING PRIMARY KEY,
                USER_ID INTEGER NOT NULL,
                PRODUCT_ID INTEGER NOT NULL,
                TRANSACTION_DATE TIMESTAMP_NTZ NOT NULL,
                AMOUNT DECIMAL(10, 2) NOT NULL,
                STATUS VARCHAR(50) NOT NULL,
                PAYMENT_METHOD VARCHAR(50) NOT NULL,
                CUSTOMER_NAME VARCHAR(100) NOT NULL,
                EMAIL VARCHAR(100) NOT NULL,
                SHIPPING_ADDRESS TEXT NOT NULL
            )
            """)
        finally:
            cursor.close()

    def prepare_record_for_insert(self, record: Dict[str, Any]) -> str:
        """Convert a single Python record to a Snowflake INSERT statement."""
        # Escape single quotes in string fields
        customer_name = record["customer_name"].replace("'", "''")
        email = record["email"].replace("'", "''")
        shipping_address = record["shipping_address"].replace("'", "''")

        # Create the values part of the INSERT statement
        values = (
            f"'{record['id']}',"
            f"{record['user_id']},"
            f"{record['product_id']},"
            f"TO_TIMESTAMP('{record['transaction_date']}'),"
            f"{record['amount']},"
            f"'{record['status']}',"
            f"'{record['payment_method']}',"
            f"'{customer_name}',"
            f"'{email}',"
            f"'{shipping_address}'"
        )

        # Complete INSERT statement
        sql = f"""
            INSERT INTO {self.table_name} (
                ID, USER_ID, PRODUCT_ID, TRANSACTION_DATE, AMOUNT,
                STATUS, PAYMENT_METHOD, CUSTOMER_NAME, EMAIL,
                SHIPPING_ADDRESS
            ) VALUES ({values})
        """

        return sql

    def execute_async_query(self, sql: str):
        """Execute a query asynchronously using Snowflake's execute_async."""
        cursor = self.connection.cursor()
        try:
            cursor.execute_async(sql)
        finally:
            cursor.close()

    def run_concurrent_inserts(
        self, inserts_per_second: int = 3000, total_records: int = 100000
    ) -> Tuple[int, float]:
        """Run concurrent inserts using Snowflake's asynchronous query execution."""
        print(
            f"Snowflake: Running {total_records} inserts with target rate of {inserts_per_second} inserts/second"
        )
        print(f"Using Snowflake's execute_async for maximum throughput")

        # Generate all records upfront
        print("Generating records...")
        records = [generate_fake_record() for _ in range(total_records)]
        print("Records generated, starting inserts...")

        # Start timer for overall throughput calculation
        overall_start = time.time()

        # Fire off all queries as fast as possible
        cursor = self.connection.cursor()
        for i, record in enumerate(records):
            sql = self.prepare_record_for_insert(record)
            cursor.execute_async(sql)


            # Print progress periodically
            if (i + 1) % 1000 == 0 or (i + 1) == total_records:
                elapsed = time.time() - overall_start
                current_rate = (i + 1) / max(elapsed, 0.001)
                print(
                    f"Snowflake progress: {i + 1}/{total_records} records inserted "
                    f"({(i + 1) / total_records * 100:.1f}%), "
                    f"Current rate: {current_rate:.2f} records/second"
                )

        # Calculate total elapsed time
        overall_time = time.time() - overall_start

        print("Waiting for any remaining queries to complete...")

        # Print final statistics
        print(f"Completed {total_records} inserts in {overall_time:.2f} seconds")
        print(f"Average throughput: {total_records / overall_time:.2f} records/second")

        return total_records, overall_time

    def count_records(self) -> int:
        """Count the number of records in the table."""
        cursor = self.connection.cursor()
        try:
            cursor.execute(f"SELECT COUNT(*) as count FROM {self.table_name}")
            result = cursor.fetchone()
            return result[0]
        finally:
            cursor.close()

    def run_benchmark(
        self, inserts_per_second: int = 3000, total_records: int = 100000
    ) -> Dict[str, Any]:
        """Run the full Snowflake benchmark."""
        print("Setting up Snowflake hybrid table...")
        self.setup_table()

        print(
            f"Starting Snowflake benchmark with target: {inserts_per_second} inserts/second"
        )
        start_time = time.time()

        try:
            # Run the concurrent inserts
            total_inserted, insert_time = self.run_concurrent_inserts(
                inserts_per_second=inserts_per_second, total_records=total_records
            )

            total_time = time.time() - start_time

            # Count records and collect results
            actual_count = self.count_records()

            return {
                "database": "Snowflake Hybrid Tables",
                "target_inserts_per_second": inserts_per_second,
                "total_records": total_records,
                "actual_records_inserted": actual_count,
                "total_time_seconds": total_time,
                "insert_time_seconds": insert_time,
                "average_inserts_per_second": actual_count / total_time
                if total_time > 0
                else 0,
            }
        finally:
            # Close connection
            if self._connection is not None:
                self._connection.close()
                self._connection = None


if __name__ == "__main__":
    benchmark = SnowflakeBenchmark()
    benchmark.run_benchmark()
