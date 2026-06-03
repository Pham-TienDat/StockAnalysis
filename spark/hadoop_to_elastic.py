from pyspark.sql import SparkSession
import os

ES_NODES = os.environ.get("ES_NODES", "")
ES_PORT = os.environ.get("ES_PORT", "443")
ES_API_KEY = os.environ.get("ES_API_KEY", "")
ES_INDEX = os.environ.get("ES_INDEX_VN30_AGG", "vn_30_aggregated")


def total_volume_per_ticker(dataframe):
    return dataframe.groupBy("ticker").sum("volume")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("ReadFromHadoop").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    input_path = "hdfs://namenode:8020/user/root/kafka_data"
    hadoop_data = spark.read.json(input_path)
    hadoop_data.show()

    total_volume = total_volume_per_ticker(hadoop_data)
    total_volume.show()

    try:
        total_volume.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.nodes", ES_NODES) \
            .option("es.port", ES_PORT) \
            .option("es.resource", ES_INDEX) \
            .option("es.net.http.header.Authorization", f"ApiKey {ES_API_KEY}") \
            .option("es.nodes.wan.only", "true") \
            .mode("overwrite") \
            .save()
        print("Data sent to Elasticsearch successfully!")
    except Exception as e:
        print(f"Error sending to Elasticsearch: {e}")

    spark.stop()
