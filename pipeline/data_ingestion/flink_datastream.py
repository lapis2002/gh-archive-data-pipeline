import json
import os

from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer, KafkaRecordSerializationSchema, KafkaSink,
    KafkaSource)
from pyflink.datastream.formats.avro import AvroRowSerializationSchema, AvroRowDeserializationSchema

JARS_PATH = f"{os.getcwd()}/jars/"

def main():
    env = StreamExecutionEnvironment.get_execution_environment()

    # The other commented lines are for Avro format
    env.add_jars(
        f"file://{JARS_PATH}/flink-connector-kafka-1.17.1.jar",
        f"file://{JARS_PATH}/flink-avro-1.17.1.jar",
        f"file://{JARS_PATH}/flink-avro-confluent-registry-1.17.1.jar",
        f"file://{JARS_PATH}/avro-1.11.1.jar",
        f"file://{JARS_PATH}/jackson-databind-2.14.2.jar",
        f"file://{JARS_PATH}/jackson-core-2.14.2.jar",
        f"file://{JARS_PATH}/jackson-annotations-2.14.2.jar",
        f"file://{JARS_PATH}/kafka-clients-3.4.0.jar",
        f"file://{JARS_PATH}/kafka-schema-registry-client-5.3.0.jar",
    )
    #! AVRO SCHEMA DOES NOT WORK WITH PYFLINK
    # avro_schema_path = f"{os.getcwd()}/pipeline/data_ingestion/avro_schemas/schema_test.avsc"
    # with open(avro_schema_path, "r") as f:
    #     schema = f.read()

    # deserialization_schema = AvroRowDeserializationSchema(avro_schema_string=schema)

    # Define the source to take data from
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("localhost:9092")
        .set_topics("streaming_gh_events")
        .set_group_id("event-consumer-group")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Define the sink to save the processed data to
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers("http://localhost:9092")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("sink_streaming_gh_events")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    # env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source").sink_to(sink=sink)
    env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source").print()
    env.execute("flink_datastream")

if __name__ == "__main__":
    main()
