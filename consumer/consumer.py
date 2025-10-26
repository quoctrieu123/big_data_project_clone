#File này dùng để tiêu thụ dữ liệu từ Kafka và ghi vào InfluxDB sử dụng Spark Structured Streaming.
import sys, json, hdfs, findspark, os #findspark để khởi tạo môi trường Spark
from pathlib import Path #đi lên hai cấp để đến thư mục gốc dự án

path_to_utils = Path(__file__).parent.parent #đi lên hai cấp để đến thư mục gốc dự án
sys.path.insert(0, str(path_to_utils)) #thêm thư mục gốc dự án vào sys.path để có thể import module từ đó
sys.path.append("/app") #thêm đường dẫn /app vào sys.path để có thể import module từ đó

from confluent_kafka import Consumer, KafkaError #lấy Consumer và KafkaError từ confluent_kafka
from script.utils import load_environment_variables #lấy các biến môi trường
from pyspark.sql import SparkSession  #spark session dùng spark sql
#from pyspark.sql.window import Window #lấy Window để làm việc với cửa sổ trong Spark
from pyspark.sql.functions import * #lấy tất cả các hàm từ pyspark.sql.functions
from pyspark.sql.types import * #lấy tất cả các kiểu dữ liệu 
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType #lấy các kiểu dữ liệu cụ thể từ pyspark.sql.types
from datetime import datetime, timedelta #lấy datetime và timedelta từ datetime

from InfluxDBWriter import InfluxDBWriter #lấy hàm InfluxDBWriter từ file InfluxDBWriter.py để ghi dữ liệu vào InfluxDB
from dotenv import load_dotenv 
load_dotenv() #tải biến môi trường từ file .env
findspark.init() #đảm bảo spark được nhận diện nếu chạy bên ngoài spark submit
env_vars = load_environment_variables()

KAFKA_TOPIC_NAME = env_vars.get("STOCK_PRICE_KAFKA_TOPIC")
KAFKA_BOOTSTRAP_SERVERS = env_vars.get("KAFKA_BROKERS")

# Configuration for Kafka Consumer
# conf = {
#     # Pointing to brokers. Ensure these match the host and ports of your Kafka brokers.
#     'bootstrap.servers': env_vars.get("KAFKA_BROKERS"),
#     'group.id': "myGroup",  # Consumer group ID. Change as per your requirement.
#     'auto.offset.reset': 'earliest'  # Start from the earliest messages if no offset is stored.
# }
# consumer = Consumer(conf)
# # Subscribe to the topic
# consumer.subscribe([env_vars.get("STOCK_PRICE_KAFKA_TOPIC"),])

scala_version = '2.12' #phiên bản Scala tương thích với Spark 3.3.x
spark_version = '3.3.3' #phiên bản spark đang sử dụng
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:2.8.1'
] #các package cần thiết để kết nối Spark với Kafka

if __name__ == "__main__":

    spark = (
        SparkSession.builder.appName("KafkaInfluxDBStreaming") #tạo một spark session với tên ứng dụng KafkaInfluxDBStreaming
        .master("spark://spark-master:7077") #kết nối đến spark master để chạy ứng dụng
        .config("spark.jars.packages", ",".join(packages)) #thêm các package cần thiết vào cấu hình spark
        .getOrCreate()
    ) 

    spark.sparkContext.setLogLevel("ERROR") #chỉ hiện thị log lỗi để giảm bớt thông tin không cần thiết


#Đọc dữ liệu từ Kafka topic
#chỉ định dữ liệu từ kafka, khai báo topic và các server của kafka brokers
#sinh ra một dataframe có các cột: key (key của message), value (nội dung message), topic (tên topic), partition (partition của topic), offset (vị trí của message trong partition), timestamp (thời gian message được gửi)
    stockDataframe = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC_NAME) \
        .load()

    stockDataframe = stockDataframe.select(col("value").cast("string").alias("data"))  #chuyển cột value từ định dạng binary sang string và đổi tên cột thành data
    inputStream = stockDataframe.selectExpr("CAST(data as STRING)") #chuyển cột data sang định dạng string

    stock_price_schema = StructType([ #định nghĩa schema cho dữ liệu stock price
        StructField("stock", StringType(), True),
        StructField("date", TimestampType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", DoubleType(), True)
    ])

    
    stockDataframe = inputStream.select(from_json(col("data"), stock_price_schema).alias("stock_price")) #parse cột data từ định dạng JSON sang cấu trúc dữ liệu đã định nghĩa trong stock_price_schema
    expandedDf = stockDataframe.select("stock_price.*") #biến tất cả các trường trong cấu trúc stock_price thành các cột riêng biệt trong dataframe
    influxdb_writer = InfluxDBWriter('primary', 'stock-price-v1') #khởi tạo đối tượng InfluxDBWriter để ghi dữ liệu vào InfluxDB
    #influxdb_writer = InfluxDBWriter(os.environ.get("INFLUXDB_BUCKET"), os.environ.get("INFLUXDB_MEASUREMENT"))
    print("InfluxDB_Init Done")

    def process_batch(batch_df, batch_id):
        realtimeStockPrices = batch_df.select("stock_price.*") #chọn tất cả các cột từ dataframe batch_df
        for realtimeStockPrice in realtimeStockPrices.collect(): #duyệt qua từng dòng trong dataframe
            timestamp = realtimeStockPrice["date"] #lấy giá trị cột date làm timestamp
            tags = {"stock": realtimeStockPrice["stock"]} #lấy giá trị cột stock làm tag
            fields = { #lấy các giá trị của các cột còn lại làm field
                "open": realtimeStockPrice['open'], 
                "high": realtimeStockPrice['high'],
                "low": realtimeStockPrice['low'],
                "close": realtimeStockPrice['close'],
                "volume": realtimeStockPrice['volume']
            }
            influxdb_writer.process(timestamp, tags, fields) #gọi hàm process của influxdb_writer để ghi dữ liệu vào InfluxDB

            # Convert Row to a dictionary
            row_dict = realtimeStockPrice.asDict() #chuyển đổi dòng dữ liệu thành dict
            row_dict['date'] = row_dict['date'].isoformat() #chuyển đổi giá trị date sang định dạng ISO 8601 để dễ đọc
            json_string = json.dumps(row_dict) #chuyển dict thành chuỗi JSON
            print(json_string)
            print("----------------------")
            hdfs.write_to_hdfs(json_string) #ghi từng bản ghi là chuoix json vào hdfs
        print(f"Batch processed {batch_id} done!") #in thông báo hoàn thành xử lý batch
#writestream: bắt đầu giai đoạn ghi dữ liệu
#foreachBatch: xử lý mỗi micro-batch bằng hàm process_batch đã định nghĩa
#outputMode("append"): chỉ thêm dữ liệu mới vào cuối
#start(): bắt đầu quá trình ghi dữ liệu

    query = stockDataframe \
        .writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .start()

    query.awaitTermination() #giữ spark chạy vô hạn cho đến khi bị dưng