#module để ghi dữ liệu vào HDFS
import pyhdfs #python client cho hdfs
import uuid #tạo mã định danh duy nhất

hdfs = pyhdfs.HdfsClient(hosts="localhost:9870", user_name="hdfs") #kết nối đến HDFS namenode của cụm hdfs

userhomedir = hdfs.get_home_directory() #lấy thư mục home của user hdfs
# print(userhomedir)
availablenode = hdfs.get_active_namenode() #lấy thông tin namenode đang hoạt động
# print(availablenode)
# print(hdfs.listdir("/"))

hdfs.mkdirs('/data') #tạo thư mục /data trên HDFS nếu chưa tồn tại
# print(hdfs.list_status('/data'))

def write_to_hdfs(json_str): #viết chuỗi json_str vào HDFS với tên file là mã uuid
    hdfs.create("/data/{}.json".format(str(uuid.uuid1())), json_str) #tạo file mới trên HDFS với tên là mã uuid và nội dung là json_str
