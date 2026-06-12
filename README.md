# Real Estate Big Data Analytics Pipeline

Hệ thống thu thập, xử lý và trực quan hóa dữ liệu bất động sản theo thời gian thực (Real-time Pipeline) ứng dụng các công nghệ Big Data.

## 🌟 Tổng quan kiến trúc

Dự án được xây dựng với kiến trúc Microservices và luồng dữ liệu (Data Pipeline) hoàn chỉnh:

1. **Crawler**: Thu thập dữ liệu bất động sản thô.
2. **Kafka (Ingestion)**: Đẩy dữ liệu thô (JSON) lên Kafka Topics theo cơ chế Event Streaming (sử dụng KRaft mode không cần Zookeeper).
3. **Spark (Processing)**: Ứng dụng Spark Structured Streaming tiêu thụ dữ liệu từ Kafka, thực hiện làm sạch, thống kê micro-batch, và lưu kết quả vĩnh viễn (Fault-tolerant) dưới định dạng Parquet.
4. **HDFS (Storage)**: Hadoop Distributed File System (NameNode & DataNode) lưu trữ dữ liệu Parquet khổng lồ một cách phân tán.
5. **Dashboard (Analytics)**: Giao diện Web tương tác bằng Streamlit, truy vấn dữ liệu từ HDFS qua WebHDFS (PyArrow) và vẽ biểu đồ với Plotly.

## 🛠️ Công nghệ sử dụng

- **Ngôn ngữ**: Python 3.10+
- **Data Engineering**: Apache Kafka, Apache Spark (PySpark), Hadoop HDFS
- **Visualization**: Streamlit, Plotly, Pandas
- **DevOps/Deployment**: Docker, Docker Compose

## 🚀 Hướng dẫn Cài đặt & Chạy dự án

Yêu cầu hệ thống: Đã cài đặt **Docker**, **Docker Compose** và **Python 3.10+**.

Cài đặt tất cả các thư viện cần thiết bằng file `requirements.txt` nằm ở thư mục gốc:

```powershell
pip install -r requirements.txt
```

### Bước 1: Khởi động hạ tầng Big Data (HDFS & Kafka)

Khởi động cụm HDFS:

```powershell
cd hdfs
docker-compose up -d
```

Khởi động cụm Kafka (kèm Schema Registry, AKHQ):

```powershell
cd ../kafka
docker-compose up -d
```

_Đợi khoảng 30 giây - 1 phút để các container khởi động hoàn tất (Healthy)._

### Bước 2: Đẩy dữ liệu vào hệ thống (Ingestion)

Mở một terminal mới, đẩy dữ liệu thu thập được lên Kafka:

```powershell
cd kafka

python traffic_ingestion.py
```

_(Bạn có thể chạy `python traffic_ingestion.py --reset` để đẩy lại toàn bộ dữ liệu từ đầu)._

### Bước 3: Khởi chạy Spark Streaming Consumer

Mở một terminal mới, chạy PySpark để lắng nghe Kafka và ghi dữ liệu xử lý vào HDFS:

```powershell
cd ../spark

python consumer.py
```

_(Giữ Terminal này chạy để hệ thống liên tục xử lý dữ liệu realtime)._

### Bước 4: Khởi chạy Dashboard trực quan hóa

Mở một terminal mới, khởi chạy Streamlit Web App:

```powershell
cd ../dashboard

streamlit run dashboard.py
```

👉 Truy cập vào trình duyệt tại địa chỉ: [http://localhost:8501](http://localhost:8501)

## 🗂️ Cấu trúc thư mục

- `/crawler`: Chứa kịch bản thu thập dữ liệu bất động sản và thư mục `/data` chứa dữ liệu thô.
- `/hdfs`: File Docker Compose cấu hình NameNode và DataNode.
- `/kafka`: File Docker Compose cho Kafka cluster và script đẩy dữ liệu (`traffic_ingestion.py`).
- `/spark`: Script PySpark Structured Streaming (`consumer.py`).
- `/dashboard`: Giao diện Web hiển thị số liệu (`dashboard.py`).
