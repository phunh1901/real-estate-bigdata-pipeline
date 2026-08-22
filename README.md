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

## 🚀 Hướng dẫn cài đặt và chạy dự án

Yêu cầu hệ thống: Đã cài đặt **Docker**, **Docker Compose** và **Python 3.10+**.

Cài đặt tất cả các thư viện cần thiết bằng file `requirements.txt` nằm ở thư mục gốc:

```powershell
pip install -r requirements.txt
```

### Luồng khuyến nghị: Docker + HDFS

Kafka, HDFS, Spark và loader chạy trong Docker; dashboard chạy trên máy host.

#### Bước 1: Khởi động HDFS và Kafka

Khởi động cụm HDFS:

```powershell
docker compose -f hdfs/docker-compose.yml up -d
```

Khởi động cụm Kafka (kèm Schema Registry, AKHQ):

```powershell
docker compose -f kafka/docker-compose.yml up -d
```

_Đợi khoảng 30 giây - 1 phút để các container khởi động hoàn tất (Healthy)._

#### Bước 2: Đẩy dữ liệu vào Kafka bằng loader

Loader dùng Python 3.10, kết nối Kafka qua mạng Docker tại `kafka:29092` và lưu trạng thái dedup trong Docker volume:

```powershell
docker compose -f kafka/docker-compose.yml --profile loader run --rm loader
```

Để xóa trạng thái dedup và đẩy lại toàn bộ dữ liệu:

```powershell
docker compose -f kafka/docker-compose.yml --profile loader run --rm loader --reset
```

Nếu muốn chạy producer trực tiếp trên máy host, dùng `python kafka/traffic_ingestion.py`; cấu hình mặc định sẽ kết nối `localhost:9092`.

#### Bước 3: Khởi chạy Spark Streaming Consumer

Spark trong Docker sử dụng `kafka:29092` và ghi dữ liệu cùng checkpoint vào HDFS:

```powershell
docker compose -f spark/docker-compose.yaml up -d
docker compose -f spark/docker-compose.yaml logs -f spark-consumer
```

#### Bước 4: Khởi chạy Dashboard

Mở một terminal mới, khởi chạy Streamlit Web App:

```powershell
streamlit run dashboard/dashboard.py
```

👉 Truy cập vào trình duyệt tại địa chỉ: [http://localhost:8501](http://localhost:8501)

Dashboard mặc định đọc `hdfs://namenode:9000/data/real-estate` thông qua WebHDFS tại `localhost:9870`.

Mỗi Spark micro-batch được ghi vào staging và rename nguyên tử sang thư mục `batch_id=...`. Spark chỉ checkpoint khi callback thành công; state store loại trùng `list_id` xuyên batch, nên retry không tạo thêm dữ liệu trùng.

### Chế độ chạy Spark local

Nếu không muốn chạy Spark trong container, vẫn khởi động Kafka theo Bước 1 rồi chạy:

```powershell
python spark/consumer.py
```

Ở chế độ này Spark dùng `localhost:9092` và ghi Parquet vào `spark/output/real-estate`. Để dashboard đọc cùng dữ liệu local:

```powershell
$env:DASHBOARD_DATA_SOURCE="local"
$env:LOCAL_DATA_PATH="$PWD\spark\output\real-estate"
streamlit run dashboard/dashboard.py
```

Xóa hai biến môi trường trên hoặc đặt `DASHBOARD_DATA_SOURCE=hdfs` để quay lại chế độ HDFS.

Các biến cấu hình chính:

- Spark: `KAFKA_BOOTSTRAP_SERVERS`, `HDFS_NAMENODE`, `HDFS_OUTPUT_PATH`, `HDFS_CHECKPOINT_PATH`.
- Dashboard: `DASHBOARD_DATA_SOURCE`, `LOCAL_DATA_PATH`, `WEBHDFS_URL`.

## 🗂️ Cấu trúc thư mục

- `/crawler`: Chứa kịch bản thu thập dữ liệu bất động sản và thư mục `/data` chứa dữ liệu thô.
- `/hdfs`: File Docker Compose cấu hình NameNode và DataNode.
- `/kafka`: File Docker Compose cho Kafka cluster và script đẩy dữ liệu (`traffic_ingestion.py`).
- `/spark`: Script PySpark Structured Streaming (`consumer.py`).
- `/dashboard`: Giao diện Web hiển thị số liệu (`dashboard.py`).
