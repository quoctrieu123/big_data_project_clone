#file này chứa các hàm giúp crawl dữ liệu chứng khoán thời gian thực và gửi đến Kafka

from dotenv import load_dotenv
from datetime import datetime, timedelta, time, timezone
import sys
import socket #lấy hostname
from script.utils import load_environment_variables
from confluent_kafka import Producer #lấy producer từ confluent_kafka để gửi tin nhắn đến Kafka
import yfinance as yf #thư viện lấy dữ liệu chứng khoán
import json 
import time as t
from seleniumbase import Driver #tự động hóa quá trình crawl dữ liệu
import requests #thư viện gửi yêu cầu HTTP
from bs4 import BeautifulSoup #thư viện phân tích HTML để crawl dữ liệu
import json 
from datetime import datetime #thư viện xử lý thời gian
driver = Driver(browser="chrome") #khởi tạo trình duyệt chrome


load_dotenv() 
env_vars = load_environment_variables()  #tải biến môi trường từ file .env
# Configuration for Kafka Producer



# Cấu hình cho Kafka Producer
conf = {
    # Pointing to all three brokers
    'bootstrap.servers': env_vars.get("KAFKA_BROKERS"), #lấy địa chỉ các broker từ biến môi trường
    'client.id': socket.gethostname(), #sử dụng hostname của máy làm client id
    'enable.idempotence': True, #đảm bảo tính idempotence để tránh trùng lặp tin nhắn
}
producer = Producer(conf) #khởi tạo Kafka Producer với cấu hình trên


def delivery_report(err, msg): # Callback khi tin nhắn được gửi thành công hoặc thất bại
    if err is not None:
        print('Message delivery failed: {}'.format(err)) #in lỗi nếu gửi thất bại
    else:
        print('Message delivered to {} [{}]'.format(
            msg.topic(), msg.partition())) #in thông tin topic và partition đến nếu gửi thành công


def send_to_kafka(producer, topic, key, partition, message): # hàm gửi tin nhắn đến Kafka
    # Sending a message to Kafka
    # producer.produce(topic, key=key, partition=0, value=json.dumps(message).encode("utf-8"))
    #gửi tin nhắn đến topic với key (xác định partition), value và callback để báo cáo kết quả gửi
    producer.produce(topic, key=key, value=json.dumps(
        message, default= str).encode("utf-8"), callback=delivery_report) 
    producer.flush() #đảm bảo message được gửi đi ngay lập tức


def retrieve_real_time_data(producer, stock_symbol, kafka_topic): # hàm lấy dữ liệu thời gian thực và gửi đến Kafka
    # stock_symbol = 'BTC-USD,ETH-USD,USDT-USD,BNB-USD,BCC'
    stock_symbols = stock_symbol.split(",") if stock_symbol else [] #tách chuỗi stock_symbol thành danh sách các mã chứng khoán
    print(stock_symbols)
    if not stock_symbols:
        print(f"No stock symbols provided in the environment variable.")
        exit(1)
    while True: #vòng lặp vô hạn để liên tục lấy dữ liệu thời gian thực
        is_market_open_bool = True #giả sử thị trường luôn mở
        if is_market_open_bool:
            end_time = datetime.now() #lấy thời gian hiện tại làm thời gian kết thúc
            start_time = end_time - timedelta(days=1) #lấy thời gian bắt đầu là 1 ngày trước thời gian kết thúc
            for symbol_index, stock_symbol in enumerate(stock_symbols): #lặp qua từng mã chứng khoán trong danh sách
                real_time_data = yf.download(
                    stock_symbol, start=start_time, end=end_time, interval="1m") #lấy dữ liệu chứng khoán trong khoảng thời gian với khoảng cách 1 phút
                if not real_time_data.empty: #kiểm tra nếu dữ liệu không rỗng
                    # Convert and send the latest real-time data point to Kafka
                    stock_symbol_new = stock_symbol #khởi tạo biến mã chứng khoán mới
                    if '.' in stock_symbol:
                        stock_symbol_new = stock_symbol.replace('.', '-')
                    latest_data_point = real_time_data.iloc[-1] #lấy điểm dữ liệu mới nhất
                    real_time_data_point = {
                        'stock': stock_symbol_new, #mã chứng khoán
                        'date': latest_data_point.name.isoformat(), #ngày giờ của điểm dữ liệu
                        'open': latest_data_point['Open'], #giá mở cửa
                        'high': latest_data_point['High'], #giá cao nhất
                        'low': latest_data_point['Low'], #giá thấp nhất
                        'close': latest_data_point['Close'], #giá đóng cửa
                        'volume': latest_data_point['Volume'] #khối lượng giao dịch
                    }
                    print(symbol_index) #in chỉ số của mã chứng khoán trong danh sách
                    print(real_time_data_point) #in điểm dữ liệu thời gian thực
                    send_to_kafka(producer, kafka_topic, stock_symbol,
                                  symbol_index, real_time_data_point) #gửi điểm dữ liệu của từng mã chứng khoán đến Kafka
        else:
            print("Market is closing")
        #code crawl thêm dữ liệu từ vndirect
        try:

            driver.get("https://banggia.vndirect.com.vn/chung-khoan/vn30") #truy cập trang web chứa dữ liệu chứng khoán
            sourceCode = driver.page_source #lấy mã nguồn HTML của trang web
            soup = BeautifulSoup(sourceCode, "html.parser") #phân tích mã nguồn HTML bằng BeautifulSoup
            items = soup.select('#banggia-khop-lenh-body tr') #chọn tất cả các hàng trong bảng dữ liệu chứng khoán
            for row in items: #duyệt qua từng hàng trong bảng và lấy dữ liệu
                row_data = {}
                symbols = row.select('td span')
                stock = row.select_one('.has-symbol').text[1:-1] + '.VN'
                if not stock in stock_symbols:
                    continue
                stock = stock[:-3]
                for symbol in symbols:
                    row_data[symbol.get('id')] = symbol.text
                real_time_data_point = {
                    'stock': stock+'.VN',
                    'date': datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z"),
                    'open': None,
                    'high': row_data[f'{stock}ceil'],
                    'low': row_data[f'{stock}floor'],
                    'close': None,
                    'volume': None
                }
                print(real_time_data_point)
                send_to_kafka(producer, kafka_topic + '-vn', stock_symbol,
                              symbol_index, real_time_data_point) #gửi điểm dữ liệu vào kafka topic với hậu tố -vn
        except:
            print('error')
        t.sleep(5) #dừng 5 giây trước khi lặp lại để tránh gửi quá nhiều yêu cầu


# driver.quit()
