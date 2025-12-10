import requests
import threading
import random
import time
import matplotlib.pyplot as plt
import csv
from datetime import datetime

# --- НАЛАШТУВАННЯ ---
COORD_URL = "http://localhost:8001"
NUM_USERS = 50  # Потоки
DURATION_SEC = 120  # Тривалість тесту (2 хвилини)

# Глобальні змінні для статистики
req_timestamps = []
lock = threading.Lock()
running = True


def user_behavior():
    while running:
        try:
            key = f"user_{random.randint(1, 10000)}"
            start = time.time()

            if random.random() < 0.5:
                requests.post(f"{COORD_URL}/create", json={"table": "users", "key": key, "value": "test"}, timeout=2)
            else:
                requests.get(f"{COORD_URL}/read/users/{key}", timeout=2)

            # Записуємо час завершення запиту і його тривалість (Latency)
            duration = (time.time() - start) * 1000  # мс
            with lock:
                req_timestamps.append((time.time(), duration))

        except:
            pass


print(f"🚀 Starting Load Test ({NUM_USERS} users) for {DURATION_SEC} seconds...")

# Запуск потоків
threads = []
for _ in range(NUM_USERS):
    t = threading.Thread(target=user_behavior)
    t.daemon = True
    t.start()
    threads.append(t)

# Збір даних
start_time = time.time()
metrics = {"time": [], "rps": [], "latency": []}

try:
    while time.time() - start_time < DURATION_SEC:
        window_start = time.time()
        time.sleep(1)  # Чекаємо 1 секунду

        # Аналізуємо запити за останню секунду
        now = time.time()
        with lock:
            # Беремо тільки ті, що були за останню секунду
            recent = [r for r in req_timestamps if r[0] > now - 1]
            # (Можна очищати старі, щоб не їсти пам'ять, але для лаби ок)

        if recent:
            rps = len(recent)
            avg_latency = sum(r[1] for r in recent) / len(recent)
        else:
            rps = 0
            avg_latency = 0

        elapsed = int(now - start_time)
        metrics["time"].append(elapsed)
        metrics["rps"].append(rps)
        metrics["latency"].append(avg_latency)

        print(f"[{elapsed}s] RPS: {rps} | Latency: {avg_latency:.1f} ms")

except KeyboardInterrupt:
    print("\n🛑 Stopped by user.")

running = False
print("Generating graphs...")

# --- МАЛЮВАННЯ ГРАФІКІВ ---
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8), sharex=True)

# Графік 1: Latency
ax1.plot(metrics["time"], metrics["latency"], color='tab:red', label='Avg Latency (ms)')
ax1.set_ylabel('Latency (ms)')
ax1.set_title('System Response to Load Spike')
ax1.grid(True)
ax1.legend()

# Графік 2: Throughput (RPS)
ax2.plot(metrics["time"], metrics["rps"], color='tab:blue', label='Throughput (RPS)')
ax2.set_xlabel('Time (seconds)')
ax2.set_ylabel('Requests per Second')
ax2.grid(True)
ax2.legend()

# Збереження
filename = "load_test_results.png"
plt.savefig(filename)
print(f"✅ Graphs saved to {filename}")
plt.show()