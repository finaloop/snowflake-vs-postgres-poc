import random
import string
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Any


def generate_random_string(length: int = 10) -> str:
    """Generate a random string of specified length."""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def generate_random_date(start_date: datetime = datetime(2020, 1, 1),
                         end_date: datetime = datetime.now()) -> datetime:
    """Generate a random date between start_date and end_date."""
    delta = (end_date - start_date).days
    random_days = random.randint(0, delta)
    return start_date + timedelta(days=random_days)


def generate_fake_record() -> Dict[str, Any]:
    """Generate a single fake record for our benchmark."""
    return {
        "id": str(uuid.uuid4()),
        "user_id": random.randint(1, 1000000),
        "product_id": random.randint(1, 50000),
        "transaction_date": generate_random_date(),
        "amount": round(random.uniform(1.0, 1000.0), 2),
        "status": random.choice(["completed", "pending", "failed", "refunded"]),
        "payment_method": random.choice(["credit_card", "debit_card", "paypal", "bank_transfer", "crypto"]),
        "customer_name": generate_random_string(15),
        "email": f"{generate_random_string(8)}@{random.choice(['example.com', 'test.org', 'fake.net'])}",
        "shipping_address": f"{random.randint(1, 9999)} {generate_random_string(10)} St, {generate_random_string(8)}, {generate_random_string(2).upper()} {random.randint(10000, 99999)}",
        "metadata": {
            "device": random.choice(["mobile", "desktop", "tablet"]),
            "browser": random.choice(["chrome", "firefox", "safari", "edge"]),
            "source": random.choice(["direct", "organic", "referral", "social", "email"])
        }
    } 