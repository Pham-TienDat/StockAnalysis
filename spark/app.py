from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, udf, broadcast, round as spark_round, concat, lit, when, to_date, current_timestamp, regexp_replace
from pyspark.sql.types import StringType, StructType, StructField, DoubleType, LongType, ArrayType
import threading
import os

# ---------------------------------------------------------------------
# CẤU HÌNH BIẾN MÔI TRƯỜNG ELASTICSEARCH
# ---------------------------------------------------------------------
ES_ENABLED = os.environ.get("ES_ENABLED", "false").lower() == "true"
ES_NODES = os.environ.get("ES_NODES", "")
ES_PORT = os.environ.get("ES_PORT", "")
ES_API_KEY = os.environ.get("ES_API_KEY", "")
ES_INDEX_VN30 = os.environ.get("ES_INDEX_VN30", "vn_30")
ES_INDEX_REALTIME = os.environ.get("ES_INDEX_REALTIME", "stock_realtime")
ES_NODES_WAN_ONLY = os.environ.get("ES_NODES_WAN_ONLY", "true").lower() == "true"

# ---------------------------------------------------------------------
# SCHEMA DỮ LIỆU ĐẦU VÀO TỪ KAFKA
# ---------------------------------------------------------------------
STOCK_SCHEMA = ArrayType(StructType([
    StructField("time", StringType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", LongType(), True),
    StructField("ticker", StringType(), True),
]))

# ---------------------------------------------------------------------
# DANH MỤC THÔNG TIN CÔNG TY (BẢNG TĨNH ĐỂ JOIN)
# ---------------------------------------------------------------------
COMPANY_INFO = [
    ("ACB", "Asia Commercial Bank", "Banking"),
    ("BCM", "Becamex IDC", "Industrial"),
    ("BID", "BIDV", "Banking"),
    ("BVH", "Bao Viet Holdings", "Insurance"),
    ("CTG", "VietinBank", "Banking"),
    ("FPT", "FPT Corporation", "Technology"),
    ("GAS", "PV GAS", "Energy"),
    ("GVR", "Vietnam Rubber Group", "Agriculture"),
    ("DGC", "Duc Giang Chemicals", "Chemicals"),
    ("HPG", "Hoa Phat Group", "Materials"),
    ("MBB", "MBBank", "Banking"),
    ("MSN", "Masan Group", "Consumer"),
    ("MWG", "Mobile World", "Retail"),
    ("PLX", "Petrolimex", "Energy"),
    ("POW", "PV Power", "Energy"),
    ("SAB", "Sabeco", "Consumer"),
    ("SHB", "SHB Bank", "Banking"),
    ("SSB", "SeABank", "Banking"),
    ("TCB", "Techcombank", "Banking"),
    ("TPB", "TPBank", "Banking"),
    ("VCB", "Vietcombank", "Banking"),
    ("VHM", "Vinhomes", "Real Estate"),
    ("VIB", "Vietnam International Bank", "Banking"),
    ("VIC", "Vingroup", "Conglomerate"),
    ("VJC", "VietJet Air", "Airlines"),
    ("VNM", "Vinamilk", "Consumer"),
    ("VPB", "VPBank", "Banking"),
    ("VRE", "Vincom Retail", "Real Estate"),
    ("SSI", "SSI Securities", "Finance"),
    ("HDB", "HDBank", "Banking"),
]

COMPANY_SCHEMA = StructType([
    StructField("ticker", StringType(), True),
    StructField("company_name", StringType(), True),
    StructField("sector", StringType(), True),
])

# ---------------------------------------------------------------------
# ĐỊNH NGHĨA CÁC HÀM UDF NGHIỆP VỤ
# ---------------------------------------------------------------------
@udf(returnType=StringType())
def classify_volume(volume):
    """Phân loại khối lượng giao dịch"""
    if volume is None:
        return "UNKNOWN"
    if volume > 1_000_000:
        return "HIGH"
    elif volume > 100_000:
        return "MEDIUM"
    return "LOW"

@udf(returnType=StringType())
def classify_volatility(high, low):
    """Phân loại mức độ rung lắc (biến động) của phiên dựa trên biên độ High - Low"""
    if high is None or low is None or low == 0:
        return "UNKNOWN"
    gap_percent = ((high - low) / low) * 100
    if gap_percent > 4.0:
        return "HIGH_VOLATILITY"
    elif gap_percent > 1.5:
        return "NORMAL"
    else:
        return "STABLE"

# ---------------------------------------------------------------------
# HÀM GHI DỮ LIỆU SANG ELASTICSEARCH
# ---------------------------------------------------------------------
def write_to_es(df, es_index):
    writer = df.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", ES_NODES) \
        .option("es.port", ES_PORT) \
        .option("es.resource", es_index) \
        .option("es.net.http.header.Authorization", f"ApiKey {ES_API_KEY}") \
        .option("es.nodes.wan.only", str(ES_NODES_WAN_ONLY).lower()) \
        .option("es.mapping.id", "doc_id") \
        .mode("append")
    writer.save()

# ---------------------------------------------------------------------
# CẤU HÌNH WRITER CHO LUỒNG VN30 (GHI HDFS VÀ ELASTICSEARCH)
# ---------------------------------------------------------------------
def make_vn30_batch_writer(spark):
    def write_batch(batch_df, epoch_id):
        if batch_df.rdd.isEmpty():
            return

        # 1. Lưu bản tin thô gốc từ Kafka vào HDFS (Giữ nguyên cấu trúc ban đầu)
        batch_df.write.mode("append").json("hdfs://namenode:8020/user/root/kafka_data")

        # 2. Xử lý làm giàu, biến đổi nâng cao và tích hợp tính năng mới
        company_df = spark.createDataFrame(COMPANY_INFO, schema=COMPANY_SCHEMA)
        
        enriched_df = batch_df \
            .withColumn("date", to_date(col("time"))) \
            .withColumn("ingest_timestamp", current_timestamp()) \
            .withColumn("price_change", spark_round(col("close") - col("open"), 2)) \
            .withColumn("price_change_pct",
                when(col("open") > 0, spark_round(((col("close") - col("open")) / col("open") * 100), 2)).otherwise(None)) \
            .withColumn("is_green_session", col("close") > col("open")) \
            .withColumn("volume_class", classify_volume(col("volume"))) \
            .withColumn("volatility_label", classify_volatility(col("high"), col("low"))) \
            .withColumn("doc_id", concat(col("ticker"), lit("_"), regexp_replace(col("time"), " ", "T"))) \
            .join(broadcast(company_df), on="ticker", how="left")

        # Sắp xếp lại thứ tự cột tường minh trước khi xuất xưởng dữ liệu sang Elasticsearch
        final_df = enriched_df.select(
            "ticker", "doc_id", "company_name", "sector", "time", "date", "open", "high", "low", "close", 
            "volume", "price_change", "price_change_pct", "is_green_session", "volume_class", "volatility_label", "ingest_timestamp"
        )

        # 3. Đẩy sang Elasticsearch
        if ES_ENABLED:
            try:
                write_to_es(final_df, ES_INDEX_VN30)
                print(f"[vn30-ES] Batch {epoch_id}: OK")
            except Exception as e:
                print(f"[vn30-ES] Batch {epoch_id} error: {e}")
        else:
            print(f"[vn30-ES] Batch {epoch_id}: ES_ENABLED=false, skipped")

    return write_batch

# ---------------------------------------------------------------------
# CẤU HÌNH WRITER CHO LUỒNG REALTIME (HIỂN THỊ CONSOLE VÀ ELASTICSEARCH)
# ---------------------------------------------------------------------
def make_realtime_batch_writer(spark): # <-- Thêm truyền biến spark vào đây
    def write_batch(batch_df, epoch_id):
        if batch_df.rdd.isEmpty():
            return

        # Tạo DataFrame từ bảng thông tin công ty tĩnh
        company_df = spark.createDataFrame(COMPANY_INFO, schema=COMPANY_SCHEMA)

        # Áp dụng các bước biến đổi và thực hiện JOIN nâng cao
        enriched_df = batch_df \
            .withColumn("date", to_date(col("time"))) \
            .withColumn("ingest_timestamp", current_timestamp()) \
            .withColumn("price_change", spark_round(col("close") - col("open"), 2)) \
            .withColumn("price_change_pct",
                when(col("open") > 0, spark_round(((col("close") - col("open")) / col("open") * 100), 2)).otherwise(None)) \
            .withColumn("is_green_session", col("close") > col("open")) \
            .withColumn("volume_class", classify_volume(col("volume"))) \
            .withColumn("volatility_label", classify_volatility(col("high"), col("low"))) \
            .withColumn("doc_id", concat(col("ticker"), lit("_"), regexp_replace(col("time"), " ", "T"))) \
            .join(broadcast(company_df), on="ticker", how="left") # 🔥 THÊM BƯỚC JOIN NÀY

        # ĐƯA CÁC CỘT MỚI VÀO DANH SÁCH HIỂN THỊ
        final_df = enriched_df.select(
            "ticker", "company_name", "sector", "time", "open", "high", "low", "close", 
            "volume", "price_change_pct", "volume_class", "volatility_label", "doc_id"
        )

        print(f"\n===== ENRICHED REALTIME BATCH {epoch_id} =====")
        final_df.show(5, False)
        
        if ES_ENABLED:
            try:
                write_to_es(final_df, ES_INDEX_REALTIME)
                print(f"[realtime-ES] Batch {epoch_id}: OK")
            except Exception as e:
                print(f"[realtime-ES] Batch {epoch_id} error: {e}")
        else:
            print(f"[realtime-ES] Batch {epoch_id}: ES_ENABLED=false, skipped")

    return write_batch
# ---------------------------------------------------------------------
# CÁC HÀM QUẢN LÝ STREAMING TỪ KAFKA TOPICS
# ---------------------------------------------------------------------
def jobVN30Data(spark):
    kafka_params = {
        "kafka.bootstrap.servers": "kafka:9092",
        "subscribe": "vn30",
        "startingOffsets": "earliest",
        "failOnDataLoss": "false",
    }

    stock_df = spark.readStream.format("kafka").options(**kafka_params).load() \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), STOCK_SCHEMA).alias("data")) \
        .select(explode(col("data")).alias("s")).select("s.*")

    import time
    max_retries = 12
    retry_delay = 10
    query = None
    for attempt in range(max_retries):
        try:
            print(f"[vn30-stream] Đang khởi chạy stream (lần thử {attempt + 1}/{max_retries})...")
            query = stock_df.writeStream \
                .foreachBatch(make_vn30_batch_writer(spark)) \
                .option("checkpointLocation", "hdfs://namenode:8020/user/root/checkpoints_hdfs") \
                .start()
            print("[vn30-stream] Stream khởi chạy thành công!")
            break
        except Exception as e:
            print(f"[vn30-stream] Không khởi chạy được stream (lần thử {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                print(f"[vn30-stream] Đang chờ {retry_delay} giây trước khi thử lại...")
                time.sleep(retry_delay)
            else:
                raise e

    if query:
        query.awaitTermination()


def jobStockRealtimeData(spark):
    kafka_params = {
        "kafka.bootstrap.servers": "kafka:9092",
        "subscribe": "stock_realtime",
        "startingOffsets": "latest",
        "failOnDataLoss": "false",
    }

    stock_df = spark.readStream.format("kafka").options(**kafka_params).load() \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), STOCK_SCHEMA).alias("data")) \
        .select(explode(col("data")).alias("s")).select("s.*")

    import time
    max_retries = 12
    retry_delay = 10
    query = None
    for attempt in range(max_retries):
        try:
            print(f"[realtime-stream] Đang khởi chạy stream (lần thử {attempt + 1}/{max_retries})...")
            query = stock_df.writeStream \
                .foreachBatch(make_realtime_batch_writer(spark)) \
                .option("checkpointLocation", "hdfs://namenode:8020/user/root/checkpoints_realtime") \
                .start()
            print("[realtime-stream] Stream khởi chạy thành công!")
            break
        except Exception as e:
            print(f"[realtime-stream] Không khởi chạy được stream (lần thử {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                print(f"[realtime-stream] Đang chờ {retry_delay} giây trước khi thử lại...")
                time.sleep(retry_delay)
            else:
                raise e

    if query:
        query.awaitTermination()

# ---------------------------------------------------------------------
# HÀM CHẠY CHÍNH (MAIN KÍCH HOẠT ĐA LUỒNG MULTI-THREADING)
# ---------------------------------------------------------------------
if __name__ == "__main__":
    spark = SparkSession.builder.appName("KafkaToElasticsearch").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    t1 = threading.Thread(target=jobVN30Data, args=(spark,))
    t2 = threading.Thread(target=jobStockRealtimeData, args=(spark,))
    t1.start()
    t2.start()

    t1.join()
    t2.join()
