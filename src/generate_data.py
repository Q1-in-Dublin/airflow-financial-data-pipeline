from faker import Faker
import pandas as pd
import random

fake = Faker()

def generate_security_id():
    return f"XX{random.randint(1000000000, 9999999999)}"

def generate_transactions(n=500, inject_errors=False):
    rows = []
    for i in range(n):
        row = {
            "transaction_id": f"TXN{i:06d}",
            "trade_date": fake.date_between(start_date="-30d", end_date="today"),
            "customer_id": f"CUST{random.randint(1000, 9999)}",
            "account_id": f"ACC{random.randint(100000, 999999)}",
            "security_id": generate_security_id(),
            "amount": round(random.uniform(10, 50000), 2),
            "currency": random.choice(["EUR", "USD", "GBP"]),
            "country": fake.country_code(),
        }
        if inject_errors and random.random() < 0.05:
            row["amount"] = None
        if inject_errors and random.random() < 0.02:
            row["amount"] = -abs(row["amount"] or 100)
        rows.append(row)
    return pd.DataFrame(rows)

if __name__ == "__main__":
    for idx in range(1, 4):
        df = generate_transactions(n=500, inject_errors=False)
        df.to_csv(f"data/transactions_{idx:03d}.csv", index=False)

    df_dirty = generate_transactions(n=200, inject_errors=True)
    df_dirty.to_csv("data/transactions_dirty.csv", index=False)

    print("Creation completed: transactions_001~003.csv, transactions_dirty.csv")