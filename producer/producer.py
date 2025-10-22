import sys, socket #lấy hostname, xử lý sys path
from pathlib import Path #giúp lấy đường dẫn file
path_to_utils = Path(__file__).parent.parent #lấy đường dẫn đến thư mục gốc của project
sys.path.insert(0, str(path_to_utils)) #thêm đường dẫn này vào đầu danh sách mà python tìm module để có thể import utils.py

from script.utils import load_environment_variables #import hàm load_environment_variables từ script/utils.py
from confluent_kafka import Producer #lấy producer từ confluent_kafka để gửi tin nhắn đến Kafka
from dotenv import load_dotenv
from producer_utils import retrieve_real_time_data #hàm lấy dữ liệu thời gian thực và gửi đến Kafka từ producer_utils.py
load_dotenv()
env_vars = load_environment_variables()
# Configuration for Kafka Producer
conf = {
    # Pointing to all three brokers
    'bootstrap.servers': env_vars.get("KAFKA_BROKERS"),
    'client.id': socket.gethostname(),
    'enable.idempotence': True,
}
producer = Producer(conf) #khởi tạo Kafka Producer với cấu hình trên

if __name__ == '__main__':
    retrieve_real_time_data(producer,
                            env_vars.get("STOCKS"),
                            env_vars.get("STOCK_PRICE_KAFKA_TOPIC"),
                            ) #gọi hàm lấy dữ liệu thời gian thực với key là mã chứng khoán và topic lấy từ biến môi trường