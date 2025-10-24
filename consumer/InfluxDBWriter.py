#File này dùng để ghi dữ liệu vào InfluxDB (một database dạng time-series) từ các dòng dữ liệu nhận được.
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta

import influxdb_client #lấy influxdb_client để kết nối và ghi dữ liệu vào InfluxDB
from influxdb_client.client.write_api import SYNCHRONOUS #ghi theo kiểu đồng bộ
from influxdb_client import Point, WritePrecision #point: đại diện cho một điểm dữ liệu trong InfluxDB; WritePrecision: độ chính xác của timestamp
import os


class InfluxDBWriter:
    def __init__(self, bucket, measurement):
        self.bucket = bucket #tên bucket (cơ sở dữ liệu) trong InfluxDB
        self.measurement = measurement #tên measurement (bảng) trong InfluxDB
        self.client = influxdb_client.InfluxDBClient(url="http://influxdb:8086", #kêt nối đến InfluxDB server
                                                     token=os.environ.get("INFLUX_TOKEN"), #token xác thực từ biến môi trường
                                                     org=os.environ.get("INFLUX_ORG") #tên tổ chức từ biến môi trường
                                                     )
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS) #tạo write_api để ghi dữ liệu vào InfluxDB theo kiểu đồng bộ
        self.is_connected() #check kết nối

    def open(self, partition_id, epoch_id): #mở kết nối cho mỗi partition và epoch
        print("Opened %d, %d" % (partition_id, epoch_id))
        return True

    # hàm ghi dữ liệu vào InfluxDB
    def process(self, timestamp, tags, fields):
        point = Point(self.measurement) #tạo một Point mới với tên measurement đã định nghĩa

        for key, value in tags.items(): #thêm các tag vào Point
            point.tag(key, value) #thêm tag vào Point

        # Add fields to the Point
        for key, value in fields.items(): #thêm các field vào Point
            point.field(key, value) 

        point.time(timestamp, WritePrecision.S) #thêm timestamp vào Point với độ chính xác là giây
        self.write_api.write(bucket=self.bucket, record=point) #gọi hàm write để ghi Point vào InfluxDB

    def close(self, error): #đóng kết nối
        self.write_api.__del__()
        self.client.__del__()
        print("Closed with error: %s" % str(error))


    #chuyển một dòng dữ liệu thành định dạng Line Protocol của InfluxDB
    def row_to_line_protocol(measurement, tags, fields, timestamp):
        """
        Convert a row into InfluxDB Line Protocol format.

        Args:
        - measurement (str): The measurement name.
        - tags (dict): A dictionary of tag key-value pairs.
        - fields (dict): A dictionary of field key-value pairs.
        - timestamp (int): The timestamp in Unix epoch format (milliseconds).

        Returns:
        - str: The InfluxDB Line Protocol string.
        """
        # chuyển tags thành string key=value, ngăn cách bằng dấu phẩy
        tag_str = ",".join([f"{k}={v}" for k, v in tags.items()])

        # chuyển fields thành string key=value, ngăn cách bằng dấu phẩy
        field_str = ",".join([f"{k}={v}" for k, v in fields.items()])

        # kết hợp measurement, tags, fields và timestamp thành định dạng Line Protocol
        line_protocol = f"{measurement}{',' + tag_str if tag_str else ''} {field_str} {timestamp}"

        return line_protocol

    def is_connected(self): #kiểm tra kết nối đến InfluxDB
        try:
            # Attempt a simple query to test the connection
            query = f'from(bucket: "{os.environ.get("INFLUXDB_BUCKET")}") |> range(start: -1m)'
            self.client.query_api().query_data_frame(query)
            return True
        except Exception as e:
            print(f"Connection error: {str(e)}")
            return False
