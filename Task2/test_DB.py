import requests
import json

BASE_URL = "http://localhost:8080"

def create_data():
    # 1. Реєструємо таблицю
    print("Registering table...")
    resp = requests.post(f"{BASE_URL}/register_table", json={
        "name": "products",
        "partition_key_name": "product_id"
    })
    print(f"Table: {resp.status_code} - {resp.text}")

    # 2. Генеруємо дані (50 записів для тесту шардингу)
    print("\nInserting data...")
    for i in range(1, 51):
        key = f"prod_{i}"
        payload = {
            "table": "products",
            "key": key,
            "value": {
                "name": f"Item {i}",
                "price": i * 10.5,
                "in_stock": True
            }
        }
        try:
            resp = requests.post(f"{BASE_URL}/create", json=payload)
            if resp.status_code == 200:
                print(f"✅ Created {key} -> Shard: {resp.json().get('target_shard_url')}")
            else:
                print(f"❌ Failed {key}: {resp.text}")
        except Exception as e:
            print(f"Error connecting: {e}")

if __name__ == "__main__":
    create_data()