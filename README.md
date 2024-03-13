# GitHub Archive Data Pipeline

## Introduction
The project embarks on the journey of constructing a comprehensive ELT (Extract, Load, Transform) data pipeline, leveraging a powerful stack of technologies to seamlessly process and analyze data from the GitHub Archive (https://www.gharchive.org/). This endeavor combines the strengths of Pyspark, PostgreSQL, Flink, Kafka, Minio, and Airflow, offering a robust infrastructure for handling data at scale. Monitoring tools such as Prometheus and Grafana are integrated to ensure the health and performance of the data pipeline.

## Objectives
The primary objective of the project is to design and implement a resilient and efficient data pipeline capable of extracting, transforming, and delivering valuable insights from the [GitHub Archive](https://www.gharchive.org/) as well as a simulating "fake" data stream. The core result tables, including `repos`, `users`, `events`, and `organizations`, serve as the foundation for our analytics data dashboard, providing data analysts with a powerful toolset for deriving meaningful insights into GitHub repositories, user activities, events, and collaborative organizations. 

## Design

### Pipeline 
![pipeline](https://github.com/lapis2002/gh-archive-data-pipeline/assets/47402970/74d1ec05-1719-494a-b4b8-f5ad8857d80a)

### Folders Structure
```
.
├── dockers
│   ├── airflow
│   └── kafka
├── jars
├── monitoring
│   ├── elk
│   │   ├── elasticsearch
│   │   ├── extensions
│   │   │   └── filebeat
│   │   ├── kibana
│   │   ├── run_env
│   │   └── setup
│   ├── grafana
│   │   ├── config
│   │   └── dashboards
│   └── prometheus
├── pipeline
│   ├── airflow
│   │   ├── config
│   │   ├── dags
│   │   │   ├── connectors
│   │   │   └── utils
│   │   ├── logs
│   │   ├── plugins
│   │   └── resources
│   │       └── jars
│   ├── connectors
│   ├── data_ingestion
│   │   ├── avro_schemas
│   │   ├── kafka_connect
│   │   │   └── jars
│   │   └── kafka_producer
│   └── utils
└── resources
    └── sample_data
        └── 2015-01-01-15
```
`docker`: Dockerfile and Docker Compose files.

`pipeline`:
- `airflow`: houses DAG task files and utility files for batch data processing.
- `connector`: contains a file with code to create a client to MinIO and Spark wrapped in a context manager for resource management and cleanup.
- `data_ingestion`: includes file for generating fake data stream and Spark structured streaming code to handle streaming data processing.

` resources`: contains sample data and config files.
### Data Source

**Batch data**

The [GitHub Archive](https://www.gharchive.org/) comprises JSON-encoded events sourced from the GitHub API. These events are organized into hourly archives, making them easily accessible through any HTTP client. 
| Query                            | Command                                                          |
|----------------------------------|------------------------------------------------------------------|
| Activity for 1/1/2015 @ 3PM UTC  | wget https://data.gharchive.org/2015-01-01-15.json.gz            |
| Activity for 1/1/2015            | wget https://data.gharchive.org/2015-01-01-{0..23}.json.gz       |
| Activity for all of January 2015 | wget https://data.gharchive.org/2015-01-{01..31}-{0..23}.json.gz |

Data extraction from the [GitHub Archive](https://www.gharchive.org/) is scheduled daily through Apache Airflow.

**Stream data**

A Kafka producer is setup to generate data, simulating real-time events. The messages undergo Avro serialization to minimize their size before being emitted to respective topics at 10-second intervals.

![Kafka_producer](https://github.com/lapis2002/gh-archive-data-pipeline/assets/47402970/f9b24e48-b390-4140-b031-3017b2a0bd69)

### Datalake & Data Warehouse
The GitHub Archive data is retrieved, decompressed, and saved as a JSON file in local storage. Subsequently, the data undergoes transformation into the Delta Lake format and is stored in designated buckets within MinIO. For streaming data, Avro-deserialized messages from the Kafka topic are written to a Delta Lake table, residing in a corresponding MinIO bucket. The final step involves writing the processed data to PostgreSQL tables: `repos`, `users`, `events`, and `organizations` for querying in later stages.

## Setup

### Prerequisites
- Python 3.10
- Kafka
- Spark 3.3
- Delta Lake
- Airflow 2.8
- Prometheus
- Grafana

To install required packages:
```
make install
```

### Setup data infrastructure local 
Create `.env` file:
```shell
vim .env
```

```
AIRFLOW_UID=1000
AIRFLOW_PROJ_DIR=./pipeline/airflow

# Postgres
POSTGRES_DB=github_archive
POSTGRES_USER=k6
POSTGRES_PASSWORD=k6

# Minio
MINIO_BUCKET=gh-archive-sample-data
MINIO_ENDPOINT=minio:9000
MINIO_ACCESS_KEY=minio_access_key
MINIO_SECRET_KEY=minio_secret_key
```

Then, run the command to build the Docker containers
```shell
make build
```
![Docker_container](https://github.com/lapis2002/gh-archive-data-pipeline/assets/47402970/56edd511-d3ed-4f95-8aad-9096bce3ced0)

### Exposed Endpoints

Minio: `localhost:9001` (`minio_access_key`/`minio_secret_key`)

Postgres: `localhost:5432` (`k6`/`k6`)

Trino: `localhost:8080`

Airflow: `localhost:8082` (`airflow`/`airflow`)

Kafka Control Center: `localhost:9021`

Grafana: `localhost:3000` (`admin`/`admin`)

Prometheus:`localhost:9090`

Statsd-exporter: `localhost:9123`

## Detailed code walkthrough
### Airflow DAG
#### DAG Graph
![DAG_graph](https://github.com/lapis2002/gh-archive-data-pipeline/assets/47402970/03923f54-5664-474e-a8fe-9b6ca396901f)

#### Environment variables
[Airflow Variables](https://airflow.apache.org/docs/apache-airflow/stable/howto/variable.html) can also be created and managed using the UI (Admin -> Variables) or Environment Variables. The environment variable naming convention is `AIRFLOW_VAR_{VARIABLE_NAME}`, all uppercase. So if our variable key is `FOO` then the variable name should be `AIRFLOW_VAR_FOO`. 

Airflow by default masks sensitive Variables ([link](https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/mask-sensitive-values.html)), such as 'access_token', 'api_key', 'apikey', 'authorization', 'passphrase', 'passwd', 'password', 'private_key', 'secret', 'token', which means if we set any environment variable containing any of those words, then every instance of them in our logs will be replaced with `***`.

Here, I set MinIO's and Postgres's access key and secret key as masked environment variables:

```yaml
## ENVIRONMENT VARIABLES FOR AIRFLOW CONFIGURATION
AIRFLOW_VAR_POSTGRES_DB: ${POSTGRES_DB:-gh_archive}
AIRFLOW_VAR_POSTGRES_USER: ${POSTGRES_USER:-k6}
AIRFLOW_VAR_POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-k6}
AIRFLOW_VAR_MINIO_ENDPOINT: ${MINIO_ENDPOINT:-minio:9000}
AIRFLOW_VAR_MINIO_ACCESS_KEY: ${MINIO_ACCESS_KEY:-minio_access_key}
AIRFLOW_VAR_MINIO_SECRET_KEY: ${MINIO_SECRET_KEY:-minio_secret_key}
AIRFLOW_VAR_MINIO_BUCKET: ${MINIO_BUCKET:-gh-archive-sample-data}
```

To access the Variable:

```python
from airflow.models import Variable
endpoint_url = Variable.get("minio_endpoint")
```

We can also access default enviroment variables, such as:
```python
date = datetime.strptime(os.getenv("AIRFLOW_CTX_EXECUTION_DATE"), "%Y-%m-%dT%H:%M:%S.%f%z")
```
#### [Data sharing among tasks](https://docs.astronomer.io/learn/airflow-passing-data-between-tasks)
Instead of returning and passing arguments between tasks, small-size data are sharing around between tasks using `XComArg`
```python
from airflow.models.xcom_arg import XComArg
```
To add data to `XComArg`:
```python
ti.xcom_push(key="json_file_name", value=json_file_name)
ti.xcom_push(key="folder_name", value=folder_name)
ti.xcom_push(key="folder_path", value=f"{ROOT_DATA_PATH}/{folder_name}")
```

And to retrieve the data:
```python
json_file_name = ti.xcom_pull(task_ids="get_file_path", key="json_file_name")
folder_name = ti.xcom_pull(task_ids="get_file_path", key="folder_name")
```
#### Main Tasks
**`download_to_bronze`**

`get_file_path` task retrieve datetime information, dynamically organizes and creates folders based on Airflow variables. Following this, `download_to_bronze` task is responsible for downloading data from the GitHub Archive, decompressing content, and storing the processed data in the designated local storage. 

**`load_to_silver`**

`setup_minio` task creates a MinIO client to check for the existence of a bucket, creating one if necessary. Once both `setup_minio` and `download_to_bronze` tasks are completed, the subsequent `load_to_silver` task takes over, reads the JSON data and writes it to a Delta Lake table in specified bucket within the MinIO object storage using Spark.

Initially, I defined a schema that included certain mandatory non-null fields. However, during the process of writing data to the Delta Lake Format using Spark, these fields were automatically converted to nullable. This conversion occurs because when Spark reads Parquet files, all columns are automatically made nullable to ensure compatibility by default ([link](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html)), leading to the observed behavior.

![MinIO](https://github.com/lapis2002/gh-archive-data-pipeline/assets/47402970/380aa2b9-0ae9-4226-829f-e82bd1dc8a96)

**`write_tables_in_gold`**

`write_tables_in_gold` task employs Spark to read from the Delta Lake table and writes the data to the corresponding `users`, `repos`, `events`, and `organizations` tables in PostgreSQL. 

### Kafka Producer
To simulating realistic data streams, Kafka producer generates Avro serialized "fake" messages and sends them to a Kafka topic every 10 seconds, and these messages draw content from a real sample set of data records. The Avro Schema is registered in the schema registry. 
![Schema_registry](https://github.com/lapis2002/gh-archive-data-pipeline/assets/47402970/e4097967-ae26-44f9-a94c-1356482e0284)

### Spark Structured Streaming
Spark Structured Streaming is used to continuously process new data from the Kafka stream, deserialize Avro and then write to Delta Lake tables designated buckets within MinIO.

## Monitoring
To emit metrics from Airflow to Prometheus, we need to setup `statsd`. The `statsd_exporter` aggregates the metrics, converts them to the Prometheus format, and exposes them as a Prometheus endpoint. This endpoint is periodically scraped by the Prometheus server, which persists the metrics in its database. Airflow metrics stored in Prometheus can then be viewed in the Grafana dashboard.

### Configure Airflow to publish the statsd metrics
Add this configuration to `x-airflow-common`'s `environment` in `airflow-docker-compose.yaml`
```yaml
    AIRFLOW__SCHEDULER__STATSD_ON: True
    AIRFLOW__SCHEDULER__STATSD_HOST: statsd-exporter
    AIRFLOW__SCHEDULER__STATSD_PORT: 8125
    AIRFLOW__SCHEDULER__STATSD_PREFIX: airflow
```

### `statsd_exporter` converts the statsd metrics to Prometheus metrics
```yaml
  statsd-exporter:
    image: prom/statsd-exporter
    container_name: airflow-statsd-exporter
    command: "--statsd.listen-udp=:8125 --web.listen-address=:9102"
    ports:
        - 9123:9102
        - 8125:8125/udp
```

The metrics can be viewed at `localhost:9123/metrics`

```
# HELP airflow_dag_data_pipeline_get_file_path_duration Metric autogenerated by statsd_exporter.
# TYPE airflow_dag_data_pipeline_get_file_path_duration summary
airflow_dag_data_pipeline_get_file_path_duration{quantile="0.5"} 0.050082105
airflow_dag_data_pipeline_get_file_path_duration{quantile="0.9"} 0.050082105
airflow_dag_data_pipeline_get_file_path_duration{quantile="0.99"} 0.050082105
airflow_dag_data_pipeline_get_file_path_duration_sum 0.050082105
airflow_dag_data_pipeline_get_file_path_duration_count 1
```

### Prometheus server to collect the metrics
```yaml
scrape_configs:
  - job_name: 'airflow_metrics'
    static_configs:
        - targets: ['host.docker.internal:9123'] # statsd-exporter port
        labels: {'host': 'airflow'} # optional: just a way to identify the system exposing metrics
```

Check whether Prometheus is able to scrape metrics from `statsd-exporter` at `http://localhost:9090/targets`
![Prometheus](https://github.com/lapis2002/gh-archive-data-pipeline/assets/47402970/5bd36339-9fb5-4f90-8951-fd1329cb2973)

### Grafana Dashboards
![Grafana_dashboard](https://github.com/lapis2002/gh-archive-data-pipeline/assets/47402970/cac20b45-9ac8-4163-bb28-405c7e1a1095)

## Tear down infrastructure
To tear down and clean up all the resources, run:
```shell
    make tear-down
```
## Results
### Connect to Postgres using DBeaver
Add new connection in DBeaver:
- Choose Postgres
- URL: `jdbc:posgresql://localhost:5432/github_archive`
- Host: `localhost`
- Port: `5432`
- Database: `github_archive`
- User: `k6`/`k6`

### Query Data
![DBeaver](https://github.com/lapis2002/gh-archive-data-pipeline/assets/47402970/84f5cd91-9521-4417-b98f-5d89d928bc1d)

## Design Considerations
### Limitation of PyFlink
Initially, I implemented PyFlink to manage the data stream. However, challenges arose as the data stream encountered errors during the deserialization of Avro Kafka messages (as below). Furthermore, PyFlink lacked the necessary support for sinking the processed data directly into a Delta Lake table. As a result, I opted for Structured Streaming Spark for data streaming needs.

![image](https://github.com/lapis2002/gh-archive-data-pipeline/assets/47402970/585fde85-7a90-476b-a92c-d2ced79f2773)


### [Writing a Kafka Stream to Delta Lake with Spark Structured Streaming](https://delta.io/blog/write-kafka-stream-to-delta-lake/)
My current approach involves employing Spark for continuous reading from a streaming Kafka source and writing to a Delta Lake table,  which requires a cluster that’s always running. Alternatively, a more cost-effective approach involves periodic provisioning of a new cluster for incremental processing of new data from the Kafka stream.

In addition, streaming data from Kafka to Delta tables can cause lots of small files, especially when the latency is low and/or the Delta table uses a Hive-style partition key that’s not conducive for streaming. To address this, it's advisable to explore the use of the optimize command, as recommended by Delta Lake. ([link](https://delta.io/blog/2023-01-25-delta-lake-small-file-compaction-optimize/))

## Further actions 
1. Reimplement Flink Datastream using Java/Scala to resolve the limit of PyFlink with Avro Serialization/Deserialization and Delta Lake Table Sink.
2. Validate data, handle duplicated events, failured messages along the pipeline
3. Monitoring Kafka, Flink metrics
4. Deploy on Cloud to scale up the storage
5. Implement tests
