FROM python:3.8-slim

# Copy app handler code
# COPY kafka_producer/produce_json.py produce.py
COPY kafka_producer/schema.json .
COPY kafka_producer/sample_data.json .

COPY avro_schemas/schema_0.avsc ./avro_schemas/
COPY kafka_producer/produce_fake_data.py producer.py
COPY gh-archive.py ./avro_schemas/

COPY kafka_producer/run.sh .

# Install dependencies
RUN pip3 install bson==0.5.10
RUN pip3 install kafka-python==2.0.2
RUN pip3 install avro==1.11.1
RUN pip3 install pandas==1.5.1
RUN pip3 install pyarrow==10.0.1
RUN pip3 install python-schema-registry-client==2.4.1
RUN pip3 install pymongo==4.5.0

CMD [ "python3", "-u", "producer.py", "--mode", "setup", "--bootstrap_servers", "broker:29092"]
