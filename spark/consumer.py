"""
Spark Structured Streaming: Đọc tin BĐS từ Kafka -> Thống kê real-time -> Ghi kho Parquet.
Cấu hình thích ứng đa môi trường (Local Windows / Docker Container HDFS).
"""
import os
import traceback
import sys



os.environ['PYSPARK_PYTHON']        = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, count, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Import file cấu hình động của hệ thống
import config

# Schema — Khớp hoàn toàn với cấu trúc JSON do Kafka Producer gửi lên
listing_schema = StructType([
    StructField("list_id",       StringType(),  True),
    StructField("title",         StringType(),  True),
    StructField("description",   StringType(),  True),
    StructField("listing_type",  StringType(),  True),
    StructField("property_type", StringType(),  True),
    StructField("price",         DoubleType(),  True),
    StructField("price_text",    StringType(),  True),
    StructField("area_m2",       DoubleType(),  True),
    StructField("rooms",         IntegerType(), True),
    StructField("toilets",       IntegerType(), True),
    StructField("region",        StringType(),  True),
    StructField("district",      StringType(),  True),
    StructField("ward",          StringType(),  True),
    StructField("street",        StringType(),  True),
    StructField("latitude",      DoubleType(),  True),
    StructField("longitude",     DoubleType(),  True),
    StructField("posted_at",     StringType(),  True),
    StructField("url",           StringType(),  True),
    StructField("full_text",     StringType(),  True),
])

# Ghép nối đường dẫn động dựa trên môi trường cấu hình (HDFS URI hoặc Local Path)
OUTPUT_URI = f"{config.HDFS_NAMENODE}{config.HDFS_OUTPUT_PATH}"
CHECKPOINT_URI = f"{config.HDFS_NAMENODE}{config.HDFS_CHECKPOINT_PATH}"


def _batch_uri(batch_id: int, staging: bool = False) -> str:
    """Tạo đường dẫn ổn định cho một batch trong cùng filesystem với output."""
    root = OUTPUT_URI.rstrip("/\\")
    directory = "_staging" if staging else ""
    suffix = f"batch_id={batch_id:020d}"
    return f"{root}/{directory}/{suffix}" if directory else f"{root}/{suffix}"


def _path_and_filesystem(spark, uri: str):
    """Lấy Hadoop Path và đúng filesystem tương ứng với URI local/HDFS."""
    path = spark._jvm.org.apache.hadoop.fs.Path(uri)
    filesystem = path.getFileSystem(spark._jsc.hadoopConfiguration())
    return path, filesystem


def commit_batch_atomically(out_df, batch_id: int) -> int:
    """Ghi một batch idempotent bằng staging + atomic rename.

    `foreachBatch` có thể chạy lại cùng batch sau lỗi. Thư mục đích theo batch_id
    đóng vai trò commit marker: tồn tại nghĩa là batch đã được commit đầy đủ.
    """
    spark = out_df.sparkSession
    final_uri = _batch_uri(batch_id)
    staging_uri = _batch_uri(batch_id, staging=True)
    final_path, filesystem = _path_and_filesystem(spark, final_uri)
    staging_path, _ = _path_and_filesystem(spark, staging_uri)

    if filesystem.exists(final_path):
        print(f"[Batch {batch_id}] Đã commit trước đó, bỏ qua an toàn: {final_uri}")
        return 0

    if filesystem.exists(staging_path):
        if not filesystem.delete(staging_path, True):
            raise RuntimeError(f"Không thể dọn staging cũ: {staging_uri}")

    total = out_df.count()
    if total == 0:
        return 0

    (out_df.write
     .mode("overwrite")
     .partitionBy("property_type")
     .parquet(staging_uri))

    filesystem.mkdirs(final_path.getParent())
    if filesystem.rename(staging_path, final_path):
        return total

    # Một executor/query khác có thể đã commit trước trong lúc ghi staging.
    if filesystem.exists(final_path):
        filesystem.delete(staging_path, True)
        return 0

    raise RuntimeError(
        f"Không thể commit nguyên tử batch {batch_id}: {staging_uri} -> {final_uri}"
    )



# Khởi tạo Spark Session đồng bộ hạ tầng
def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(config.SPARK_APP_NAME)
        .master(config.SPARK_MASTER)
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark



# Hàm xử lý logic cho từng Micro-Batch (Mỗi 10 giây)
def process_and_save_batch(batch_df, batch_id):
    """Phân tích micro-batch và commit Parquet theo batch một cách idempotent."""
    
    batch_df.cache()
    try:
        total = batch_df.count()
        if total == 0:
            print(f"\n[Batch {batch_id}]: Không có dữ liệu mới trong 10 giây qua.")
            return

        print(f"\n{'='*60}\nBATCH {batch_id} - BÁO CÁO PHÂN TÍCH BẤT ĐỘNG SẢN REAL-TIME\n{'='*60}")
        print(f"Tổng số tin tiếp nhận: {total}")

        # Thống kê 1: Phân loại theo hình thức (Bán / Cho thuê)
        print("\nTheo hình thức thị trường:")
        for r in (batch_df.groupBy("listing_type")
                  .agg(count("*").alias("n"))
                  .orderBy(col("n").desc()).collect()):
            print(f"  • {str(r['listing_type'])[:15]:15s}: {r['n']:5d} tin")

        # Thống kê 2: Phân loại theo loại hình sản phẩm (Chung cư, Nhà ở, Đất nền...)
        print("\nTheo loại hình bất động sản:")
        for r in (batch_df.groupBy("property_type")
                  .agg(count("*").alias("n"))
                  .orderBy(col("n").desc()).collect()):
            print(f"  • {str(r['property_type'])[:30]:30s}: {r['n']:5d} tin")

        # Thống kê 3: Top 10 Khu vực Quận/Huyện sôi động nhất
        print("\nTop 10 khu vực biến động nguồn cung lớn nhất:")
        for r in (batch_df.filter(col("district") != "")
                  .groupBy("district").agg(count("*").alias("n"))
                  .orderBy(col("n").desc()).limit(10).collect()):
            print(f"  • {str(r['district'])[:30]:30s}: {r['n']:5d} tin")

        # Thống kê 4: Phân tích biên độ giá (Chỉ tính định dạng "Bán" và giá trị hợp lệ)
        priced = batch_df.filter(
            (col("listing_type") == "Bán") &
            col("price").isNotNull() & (col("price") > 0)
        )
        if priced.count() > 0:
            s = priced.selectExpr("min(price) mn", "max(price) mx", "avg(price) av").collect()[0]
            med = priced.approxQuantile("price", [0.5], 0.05)
            print("\nBiên độ GIÁ BÁN thị trường (tỷ VND):")
            print(f"  • Thấp nhất : {s['mn']/1e9:.2f}")
            print(f"  • Cao nhất  : {s['mx']/1e9:.2f}")
            print(f"  • Trung bình: {s['av']/1e9:.2f}")
            if med:
                print(f"  • Trung vị  : {med[0]/1e9:.2f}")

        # Thống kê 5: Phân tích diện tích (m²)
        sized = batch_df.filter(col("area_m2").isNotNull() & (col("area_m2") > 0))
        if sized.count() > 0:
            a = sized.selectExpr("avg(area_m2) av", "min(area_m2) mn", "max(area_m2) mx").collect()[0]
            print(f"\nThông số diện tích (m²): Trung bình {a['av']:.1f} | Nhỏ nhất {a['mn']:.0f} | Lớn nhất {a['mx']:.0f}")


        # Loại trùng trong batch; loại trùng xuyên batch được state store xử lý.
        out_df = (batch_df
                  .dropDuplicates(["list_id"])
                  .withColumn("processed_at", current_timestamp()))

        n_new = commit_batch_atomically(out_df, batch_id)
        if n_new == 0:
            print("\n  Không có dữ liệu mới hoặc batch đã được commit trước đó.")
            return
        
        print(f"\n Đã commit nguyên tử {n_new} tin mới vào: {_batch_uri(batch_id)}")

    except Exception as e:
        print(f" Xảy ra sự cố xử lý batch {batch_id}: {e}")
        traceback.print_exc()
        # Bắt buộc báo thất bại để Spark không checkpoint/advance offset của batch lỗi.
        raise
    finally:
        # Giải phóng bộ nhớ RAM sau khi kết thúc một chu kỳ Micro-batch
        batch_df.unpersist()



def main():
    spark = create_spark_session()
    try:
        # Khởi tạo luồng nhận dữ liệu (Stream Reader) từ Kafka Broker
        kafka_df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", config.KAFKA_TOPIC)
            .option("startingOffsets", config.KAFKA_STARTING_OFFSET)
            .option("failOnDataLoss", "false")
            .option("kafka.isolation.level", "read_committed")
            .load()
        )

        # Định hình cấu trúc (Parsing) từ chuỗi JSON nhị phân của Kafka sang cấu trúc cột phẳng
        parsed_df = (
            kafka_df
            .select(
                from_json(col("value").cast("string"), listing_schema).alias("d"),
                col("timestamp").alias("kafka_ts"),
                col("topic").alias("kafka_topic"),
                col("partition").alias("kafka_partition"),
                col("offset").alias("kafka_offset"),
            )
            .select(
                "d.*", "kafka_ts", "kafka_topic", "kafka_partition", "kafka_offset"
            )
            # Bộ lọc sơ bộ loại bỏ tin rác khuyết tiêu đề hoặc mã định danh
            .filter(col("list_id").isNotNull() & col("title").isNotNull())
            # State store + checkpoint bảo đảm một list_id chỉ đi qua một lần.
            .dropDuplicates(["list_id"])
        )

        # Kích hoạt trạm đẩy dữ liệu (Stream Writer) theo chu kỳ 10 giây một lần
        query = (
            parsed_df.writeStream
            .foreachBatch(process_and_save_batch)
            .outputMode("append")
            # ÉP BUỘC: Đưa đường dẫn checkpoint động vào writeStream để đảm bảo tính sẵn sàng cao (Fault Tolerance)
            .option("checkpointLocation", CHECKPOINT_URI)
            .trigger(processingTime="10 seconds")
            .start()
        )
        
        print(f" Spark Structured Streaming đã khởi động thành công!")
        print(f"   • Đang nghe cổng Kafka: {config.KAFKA_BOOTSTRAP_SERVERS}")
        print(f"   • Kho lưu trữ cấu hình: {OUTPUT_URI}")
        print(f"   • Trạm ghi nhớ Checkpoint: {CHECKPOINT_URI}")
        print("▶ Press Ctrl+C to terminate system...")
        
        query.awaitTermination()

    except KeyboardInterrupt:
        print("\n  Nhận lệnh dừng hệ thống từ người dùng. Đang đóng luồng dữ liệu...")
    except Exception as e:
        print(f" Hệ thống gặp sự cố chí mạng: {e}")
        traceback.print_exc()
        raise
    finally:
        spark.stop()
        print(" Đã giải phóng hoàn toàn Spark Session. Hệ thống tắt an toàn.")


if __name__ == "__main__":
    main()
