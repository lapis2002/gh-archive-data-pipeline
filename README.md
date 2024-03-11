# GitHub Archive Data Pipeline

## Introduction

## Objectives

## Design

### Pipeline 
#### Source
##### Batch data
`https://www.gharchive.org/`: Each archive contains JSON encoded events as reported by the GitHub API, and these events are aggregated into hourly archives, which we can access with any HTTP client

| Query                            | Command                                                          |
|----------------------------------|------------------------------------------------------------------|
| Activity for 1/1/2015 @ 3PM UTC  | wget https://data.gharchive.org/2015-01-01-15.json.gz            |
| Activity for 1/1/2015            | wget https://data.gharchive.org/2015-01-01-{0..23}.json.gz       |
| Activity for all of January 2015 | wget https://data.gharchive.org/2015-01-{01..31}-{0..23}.json.gz |


Data will be pulled from https://www.gharchive.org/ every day scheduled by Airflow.

##### Stream data
We setup a Kafka producer to produce data, silmulaing real-time data. Messages are Avro serialized to reduce the size and emitted to topics every 10 second.

### Datalake & Datawarehouse

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

## Setup

### Prequisites

### Setup data infrastructure local 

### Exposed Endpoints

Minio: `localhost:9001/` (`minio_access_key`/`minio_secret_key`)

Postgres: `localhost:5432` (`k6`/`k6`)

Trino: `localhost:8080`

Airflow: `localhost:8082` (`airflow`/`airflow`)

Kafka Control Center: `localhost:9021`

Grafana: `localhost:3000` (`admin`/`admin`)

Prometheus:`localhost:9090/`

Statsd-exporter: `localhost:9123`

## Detailed code walkthrough
### Airflow DAG
#### DAG Graph

#### Environment variables
[Airflow Variables](https://airflow.apache.org/docs/apache-airflow/stable/howto/variable.html) can also be created and managed using the UI (Admin -> Variables) or Environment Variables. The environment variable naming convention is `AIRFLOW_VAR_{VARIABLE_NAME}`, all uppercase. So if our variable key is `FOO` then the variable name should be `AIRFLOW_VAR_FOO`. 

Airflow by default masks sensitive Variables ([link](https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/mask-sensitive-values.html)), such as 'access_token', 'api_key', 'apikey','authorization', 'passphrase', 'passwd', 'password', 'private_key', 'secret', 'token', which means if we have a connection with password/secret, then every instance of them in our logs will be replaced with `***`.

Here, we set minIO's and Postgres's access key and secret key as masked environment variables:

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
#### Passing data among tasks
Instead of returning and passing arguments between tasks, data are parsing around between tasks using `XComArg`
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
##### `download_to_bronze`

##### `load_to_silver`

##### `write_tables_in_gold`

#### Monitoring
To emit metrics from Airflow to Prometheus, we need to setup `statsd`. The `statsd_exporter` aggregates the metrics, converts them to the Prometheus format, and exposes them as a Prometheus endpoint. This endpoint is periodically scraped by the Prometheus server, which persists the metrics in its database. Airflow metrics stored in Prometheus can then be viewed in the Grafana dashboard.

##### Configure Airflow to publish the statsd metrics
Add this configuration to `x-airflow-common`'s `environment` in `airflow-docker-compose.yaml`
```yaml
    AIRFLOW__SCHEDULER__STATSD_ON: True
    AIRFLOW__SCHEDULER__STATSD_HOST: statsd-exporter
    AIRFLOW__SCHEDULER__STATSD_PORT: 8125
    AIRFLOW__SCHEDULER__STATSD_PREFIX: airflow
```

##### `statsd_exporter` converts the statsd metrics to Prometheus metrics
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

##### Prometheus server to collect the metrics
```
scrape_configs:
  - job_name: 'airflow_metrics'
    static_configs:
        - targets: ['host.docker.internal:9123'] # statsd-exporter port
        labels: {'host': 'airflow'} # optional: just a way to identify the system exposing metrics
```

Check whether Prometheus is able to scrape metrics from `statsd-exporter` at `http://localhost:9090/targets`

##### Grafana Dashboards

## Tear down infrastructure
To tear down and clean up all the resources, run:
```shell
    make tear-down
```

## Further actions 
1. Reimplement Flink Datastream using Java/Scala to resolve the limit of PyFlink with Avro Serialization/Deserialization and Delta Lake Table Sink.
2. Validate data, handle duplicated events, failured messages along the pipeline
3. Monitoring Kafka, Flink metrics
4. Deploy on Cloud to scale up the storage
5. Implement tests