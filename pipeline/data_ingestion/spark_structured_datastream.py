import io


from schema_registry.client import SchemaRegistryClient, schema

from pyspark.sql.functions import col
from pyspark.sql.avro.functions import from_avro

from pyspark.sql import SparkSession

// from connectors import spark_context_manager

def get_schema_from_schema_registry(schema_registry_url, schema_registry_subject):
    sr = SchemaRegistryClient({'url': schema_registry_url})
    latest_version = sr.get_latest_version(schema_registry_subject)

    return sr, latest_version

def main():
    # bytes_io = io.BytesIO(msg)
    # bytes_io.seek(0)
    # msg_decoded = fastavro.schemaless_reader(bytes_io, schema)

    # session = SparkSession.builder \
    #                   .appName("Kafka Spark Streaming Avro example") \
    #                   .getOrCreate()

    # streaming_context = StreamingContext(sparkContext=session.sparkContext,
    #                                     batchDuration=5)

    # kafka_stream = KafkaUtils.createDirectStream(ssc=streaming_context,
    #                                             topics=['your_topic_1', 'your_topic_2'],
    #                                             kafkaParams={"metadata.broker.list": "your_kafka_broker_1,your_kafka_broker_2"},
    #                                             valueDecoder=decoder)

    spark = (
        SparkSession.builder.master("local[*]")
        .config(
            "spark.jars",
            """jars/postgresql-42.6.0.jar,
               jars/deequ-2.0.3-spark-3.3.jar,
               jars/hadoop-aws-2.8.0.jar,
               jars/spark-sql-kafka-0-10_2.12-3.3.2.jar,
               jars/kafka-clients-2.7.2.jar,
               jars/spark-streaming_2.12-3.3.2.jar,
               jars/spark-streaming-kafka-0-10-assembly_2.12-3.3.2.jar,
               jars/spark-token-provider-kafka-0-10_2.12-3.3.2.jar,
               jars/commons-pool2-2.10.0.jar""",
        )
        .getOrCreate()
    )

    df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "http://localhost:9092") \
            .option("subscribe", "streaming_gh_events") \
            .option("subscribe", "avro_streaming_gh_events") \
            .load()
            # .select(
            #     from_avro(
            #         data = col("value"), 
            #         subject = "t-value",
            #         schemaRegistryAddress = "http://schema-registry:8081"
            #     ).alias("value")
            # )    
    bucket = "gh-archive-sample-data"
    folder_name = "2015-01-02"
    outputPath = f"s3a://{bucket}/{folder_name}"
    # query = df.writeStream.format("console").start()
    query = df.writeStream\
        .format("delta")\
        .outputMode("append")\
        .option("checkpointLocation", "/tmp/delta/events/_checkpoints/")\
        .toTable(outputPath)
    import time
    time.sleep(60) # sleep 10 seconds
    query.stop()
if __name__ == "__main__":
    main()