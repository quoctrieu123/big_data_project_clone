#File này dùng để kiểm tra consumer Kafka bằng cách nhận và in các tin nhắn từ một topic cụ thể.
import sys, socket
from pathlib import Path
path_to_utils = Path(__file__).parent.parent #đi lên hai cấp để đến thư mục gốc dự án
sys.path.insert(0, str(path_to_utils)) #thêm thư mục gốc dự án vào sys.path để có thể import module từ đó

from confluent_kafka import Consumer, KafkaError #lấy Consumer và KafkaError từ confluent_kafka
from script.utils import load_environment_variables #lấy các biến môi trường
from dotenv import load_dotenv #tải biến môi trường từ file .env

load_dotenv() #tải biến môi trường từ file .env
env_vars = load_environment_variables() #đict các biến môi trường từ file .env
KAFKA_BROKERS = "localhost:9092,localhost:9093,localhost:9094"
# Cấu hình cho Kafka Consumer
conf = {
    # Pointing to brokers. Ensure these match the host and ports of your Kafka brokers.
    'bootstrap.servers': KAFKA_BROKERS, #lấy địa chỉ các broker từ biến môi trường
    'group.id': "myGroup",  #các consumer trong cùng một group sẽ chia sẻ công việc đọc tin nhắn từ tất cả các partition
    'auto.offset.reset': 'earliest'  # bắt đầu đọc từ đầu topic nếu không có offset đã lưu
#latest: bắt đầu đọc message từ thời điểm đã lưu
}
consumer = Consumer(conf) #khởi tạo Kafka Consumer với cấu hình trên
# subscribe topic để lấy tin nhắn
consumer.subscribe([env_vars.get("STOCK_PRICE_KAFKA_TOPIC"),])

try:
    # Continuously poll for new messages
    while True:
        msg = consumer.poll(timeout=1.0)  #trả về None nếu trong 1 giây không có tin nhắn mới, nếu có tin nhắn mới sẽ trả về đối tượng message

        # Check for message. If none, continue polling
        if msg is None:
            continue #nếu không có tin nhắn thì tiếp tục vòng lặp

        
        if msg.error(): #nếu message trả về có lỗi
            # Continue if it's an end of partition event
            if msg.error().code() == KafkaError._PARTITION_EOF: #kiểm tra nếu lỗi là do đã đọc hết tin nhắn trong partition
                continue
            else:
                # Print any other error and break from the loop
                print(msg.error()) #in lỗi nếu có lỗi khác
                break

        # Decode and print the message received
        print('Received message: {}'.format(msg.value().decode('utf-8'))) #in nội dung tin nhắn đã giải mã từ bytes sang string

# Allow for graceful shutdown on interrupt
except KeyboardInterrupt:
    pass

finally:
    # Close down the consumer to commit final offsets.
    consumer.close() #commit offset cuối cùng và đóng consumer
