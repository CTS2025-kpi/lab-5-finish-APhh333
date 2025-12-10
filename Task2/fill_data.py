import requests
COORD = "http://localhost:8080"
# Реєструємо таблицю
requests.post(f"{COORD}/register_table", json={"name": "users", "partition_key_name": "id"})
# Заливаємо 1000 ключів
print("Заливаю 1000 ключів...")
for i in range(1000):
    requests.post(f"{COORD}/create", json={
        "table": "users", "key": f"user_{i}", "value": "test"
    })
print("Готово!")