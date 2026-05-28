from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, udf, broadcast, round as spark_round, concat, lit
from pyspark.sql.types import StringType, StructType, StructField, DoubleType, LongType, ArrayType
import threading
import os

ES_ENABLED = os.environ.get("ES_ENABLED", "false").lower() == "true"
ES_NODES = os.environ.get("ES_NODES", "https://big-data.es.asia-southeast1.gcp.elastic-cloud.com")
ES_PORT = os.environ.get("ES_PORT", "9243")
ES_USER = os.environ.get("ES_USER", "elastic")
ES_PASSWORD = os.environ.get("ES_PASSWORD", "")
ES_INDEX_VN30 = os.environ.get("ES_INDEX_VN30", "vn_30")
ES_INDEX_REALTIME = os.environ.get("ES_INDEX_REALTIME", "stock_realtime")

STOCK_SCHEMA = ArrayType(StructType([
    StructField("time", StringType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", LongType(), True),
    StructField("ticker", StringType(), True),
]))

# Reference data for broadcast join (sector enrichment)
COMPANY_INFO = [
    ("ACB", "Asia Commercial Bank", "Banking"),
    ("BCM", "Becamex IDC", "Industrial"),
    ("BID", "BIDV", "Banking"),
    ("BVH", "Bao Viet Holdings", "Insurance"),
    ("CTG", "VietinBank", "Banking"),
    ("FPT", "FPT Corporation", "Technology"),
    ("GAS", "PV GAS", "Energy"),
    ("GVR", "Vietnam Rubber Group", "Agriculture"),
    ("DHB", "Duc Giang Chemicals", "Chemicals"),
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


@udf(returnType=StringType())
def classify_volume(volume):
    if volume is None:
        return "UNKNOWN"
    if volume > 1_000_000:
        return "HIGH"
    elif volume > 100_000:
        return "MEDIUM"
    return "LOW"


def write_to_es(df, es_index):
    df.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", ES_NODES) \
        .option("es.port", ES_PORT) \
        .option("es.resource", es_index) \
        .option("es.net.http.auth.user", ES_USER) \
        .option("es.net.http.auth.pass", ES_PASSWORD) \
        .option("es.nodes.wan.only", "true") \
        .option("es.mapping.id", "doc_id") \
        .mode("append") \
        .save()


def make_vn30_batch_writer(spark):
    def write_batch(batch_df, epoch_id):
        if batch_df.rdd.isEmpty():
            return

        # 1. Append raw records to HDFS
        batch_df.write.mode("append").json("hdfs://namenode:8020/user/root/kafka_data")

        # 2. Enrich: price change %, volume classification, sector via broadcast join
        company_df = spark.createDataFrame(COMPANY_INFO, schema=COMPANY_SCHEMA)
        enriched_df = batch_df \
            .withColumn("price_change_pct",
                spark_round(((col("close") - col("open")) / col("open") * 100), 2)) \
            .withColumn("volume_class", classify_volume(col("volume"))) \
            .join(broadcast(company_df), on="ticker", how="left") \
            .withColumn("doc_id", concat(col("ticker"), lit("_"), col("time")))

        # 3. Write enriched records to Elasticsearch (requires ES_ENABLED=true)
        if ES_ENABLED:
            try:
                write_to_es(enriched_df, ES_INDEX_VN30)
                print(f"[vn30-ES] Batch {epoch_id}: OK")
            except Exception as e:
                print(f"[vn30-ES] Batch {epoch_id} error: {e}")
        else:
            print(f"[vn30-ES] Batch {epoch_id}: ES_ENABLED=false, skipped")

    return write_batch


def make_realtime_batch_writer():
    def write_batch(batch_df, epoch_id):
        if batch_df.rdd.isEmpty():
            return

        enriched_df = batch_df \
            .withColumn("price_change_pct",
                spark_round(((col("close") - col("open")) / col("open") * 100), 2)) \
            .withColumn("volume_class", classify_volume(col("volume"))) \
            .withColumn("doc_id", concat(col("ticker"), lit("_"), col("time")))

        if ES_ENABLED:
            try:
                write_to_es(enriched_df, ES_INDEX_REALTIME)
                print(f"[realtime-ES] Batch {epoch_id}: OK")
            except Exception as e:
                print(f"[realtime-ES] Batch {epoch_id} error: {e}")
        else:
            print(f"[realtime-ES] Batch {epoch_id}: ES_ENABLED=false, skipped")

    return write_batch


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

    query = stock_df.writeStream \
        .foreachBatch(make_vn30_batch_writer(spark)) \
        .option("checkpointLocation", "hdfs://namenode:8020/user/root/checkpoints_hdfs") \
        .start()

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

    query = stock_df.writeStream \
        .foreachBatch(make_realtime_batch_writer()) \
        .option("checkpointLocation", "hdfs://namenode:8020/user/root/checkpoints_realtime") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    spark = SparkSession.builder.appName("KafkaToElasticsearch").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    t1 = threading.Thread(target=jobVN30Data, args=(spark,))
    t2 = threading.Thread(target=jobStockRealtimeData, args=(spark,))
    t1.start()
    t2.start()

    t1.join()
    t2.join()
