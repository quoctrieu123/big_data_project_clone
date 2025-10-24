import sys, json
from pathlib import Path
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
from tabulate import tabulate

# Thiết lập đường dẫn import
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0,str(ROOT_DIR))
from script.utils import load_environment_variables

# Load biến môi trường
load_dotenv()
env_vars = load_environment_variables()

# Cấu hình Kafka Consumer
conf = {
    'bootstrap.servers': env_vars.get("KAFKA_BROKERS"),
    'group.id': "myGroup-1",
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)

# Đăng ký topic
topic = env_vars.get("STOCK_PRICE_KAFKA_TOPIC")
consumer.subscribe([topic])

print(f"\n📡 Đang lắng nghe dữ liệu từ topic: {topic}\n")

try:
    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Lỗi Kafka: {msg.error()}")
                break

        # Giải mã message
        raw_value = msg.value().decode('utf-8')

        try:
            data = json.loads(raw_value)  # nếu là JSON
            # Nếu là dict → in dạng bảng
            headers = list(data.keys())
            row = [data.values()]
            print(tabulate(row, headers=headers, tablefmt="fancy_grid"))
        except json.JSONDecodeError:
            # Nếu không phải JSON → in raw
            print(f"🔹 Message: {raw_value}")

except KeyboardInterrupt:
    print("\n🛑 Dừng consumer...")
finally:
    consumer.close()
    print("✅ Đã đóng kết nối consumer.")
