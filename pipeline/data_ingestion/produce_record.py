def create_topic(admin, topic_name):
    # Create topic if not exists
    try:
        # Create Kafka topic
        topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
        admin.create_topics([topic])
        print(f"A new topic {topic_name} has been created!")
    except Exception:
        print(f"Topic {topic_name} already exists. Skipping creation!")
        pass
    
def create_streams(servers, avro_schemas_path, schema_registry_client):
    producer = None
    admin = None
    for i in range(10):
        try:
            producer = KafkaProducer(bootstrap_servers=servers)
            admin = KafkaAdminClient(bootstrap_servers=servers)
            print("SUCCESS: instantiated Kafka admin and producer")
            break
        except Exception as e:
            print(
                f"Trying to instantiate admin and producer with bootstrap servers {servers} with error {e}"
            )
            sleep(10*i)
            pass

    record = {}
    # Make event one more year recent to simulate fresher data
    record["created"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    #! Read record from request
    #! verify record
    
    # Read columns from schema
    with open(avro_schema_path, "r") as f:
        parsed_avro_schema = json.loads(f.read())

    # serialize the message data using the schema
    avro_schema = avro.schema.parse(open(avro_schema_path, "r").read())
    writer = avro.io.DatumWriter(avro_schema)
    bytes_writer = io.BytesIO()
    # Write the Confluence "Magic Byte"
    bytes_writer.write(bytes([0]))

    # Get topic name for this device
    topic_name = record["type"]

    # Check if schema exists in schema registry,
    # if not, register one
    schema_version_info = schema_registry_client.check_version(
        f"{topic_name}-value", schema.AvroSchema(parsed_avro_schema)
    )
    if schema_version_info is not None:
        schema_id = schema_version_info.schema_id
        print(
            "Found an existing schema ID: {}. Skipping creation!".format(schema_id)
        )
    else:
        schema_id = schema_registry_client.register(
            f"{topic_name}-value", schema.AvroSchema(parsed_avro_schema)
        )

    # Write schema ID
    bytes_writer.write(int.to_bytes(schema_id, 4, byteorder="big"))

    # Write data
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write(record, encoder)

    # Create a new topic if not exists
    create_topic(admin, topic_name=topic_name)

    # Send messages to this topic
    producer.send(topic_name, value=bytes_writer.getvalue(), key=None)

    
def teardown_stream(topic_name, servers=["localhost:9092"]):
    try:
        admin = KafkaAdminClient(bootstrap_servers=servers)
        print(admin.delete_topics([topic_name]))
        print(f"Topic {topic_name} deleted")
    except Exception as e:
        print(str(e))
        pass
    