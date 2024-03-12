import requests
import gzip
import os
import shutil
from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.xcom_arg import XComArg
from airflow.models import Variable

from pyspark.sql.functions import col

from connectors import minio_manager, spark_context_manager
from schema import GH_ARCHIVE_SCHEMA

ROOT_DATA_PATH = "/opt/airflow/resources"


@task()
def setup_minio(ti=None) -> None:
    minio_conf = {
        "endpoint_url": Variable.get("minio_endpoint"),
        "access_key": Variable.get("minio_access_key"),
        "secret_key": Variable.get("minio_secret_key"),
    }

    bucket = Variable.get("minio_bucket")

    print("Checking if the bucket exists...")
    with minio_manager.get_minio_client(minio_conf) as client:
        found = client.bucket_exists(bucket_name=bucket)
        if not found:
            client.make_bucket(bucket_name=bucket)
        else:
            print(f"Bucket {bucket} already exists, skip creating!")


@task(multiple_outputs=True)
def get_file_path(ti=None) -> None:
    date = datetime.strptime(
        os.getenv("AIRFLOW_CTX_EXECUTION_DATE"), "%Y-%m-%dT%H:%M:%S.%f%z"
    )
    # since we cannot get the data for the current year, use the previous 5 years
    year = str(int(date.year) - 10)
    month = date.month
    day = date.day
    hour = date.hour
    folder_name = f"{year}-{month:02d}-{day:02d}"
    folder_path = f"{ROOT_DATA_PATH}/{folder_name}"

    json_file_name = f"{year}-{month:02d}-{day:02d}-{hour}.json"
    
    
    file_path_config = {
        "json_file_name": json_file_name,
        "folder_name": folder_name
    }

    file_path_config = {
        "json_file_name": json_file_name,
        "folder_name": folder_name
    }
    ti.xcom_push(key="json_file_name", value=json_file_name)
    ti.xcom_push(key="folder_name", value=folder_name)
    ti.xcom_push(key="folder_path", value=f"{ROOT_DATA_PATH}/{folder_name}")

    isExist = os.path.exists(folder_path)
    if not isExist:
        # Create a new directory because it does not exist
        os.makedirs(folder_path)


@task()
def download_to_bronze(ti=None) -> None:
    json_file_name = ti.xcom_pull(task_ids="get_file_path", key="json_file_name")
    folder_name = ti.xcom_pull(task_ids="get_file_path", key="folder_name")

    headers = {"User-Agent": "Mozilla/5.0"}

    # hour is 0-23
    url = f"https://data.gharchive.org/{json_file_name}.gz"

    print(f"Extracting data from {url}...")
    data = requests.get(url, headers=headers)
    if data.status_code != 200:
        raise ValueError(f"Failed to extract data from {url}")

    print(f"Data extracted from {url}")

    json_data = gzip.decompress(data.content)
    json_data_decoded = json_data.decode("utf-8")

    json_file_path = f"{ROOT_DATA_PATH}/{folder_name}/{json_file_name}"

    print(f"Writing data to {json_file_path}...")
    with open(json_file_path, "w") as f:
        f.write(json_data_decoded)

    print(f"Data extracted to {json_file_path}")


@task()
def load_to_silver(ti=None) -> None:
    json_file_name = ti.xcom_pull(task_ids="get_file_path", key="json_file_name")
    folder_name = ti.xcom_pull(task_ids="get_file_path", key="folder_name")

    json_file_path = f"{ROOT_DATA_PATH}/{folder_name}/{json_file_name}"

    minio_conf = {
        "endpoint_url": Variable.get("minio_endpoint"),
        "access_key": Variable.get("minio_access_key"),
        "secret_key": Variable.get("minio_secret_key"),
    }

    bucket = Variable.get("minio_bucket")

    with spark_context_manager.get_spark_session({}, "data_lake") as spark:
        print("Spark session created")
        print("*" * 30)
        spark_context_manager.load_minio_config(spark.sparkContext, minio_conf)

        print("set SparkContext")

        print("*" * 30)
        print(f"Reading data from {json_file_path}...")
        #^ Error: Change all Schema fields to nullable
        df = spark\
                .read\
                .schema(GH_ARCHIVE_SCHEMA)\
                .option("timestampNTZFormat", "yyyy-MM-dd'T'HH:mm:ss'Z'")\
                .json(json_file_path)
        df.show(10)

        #! Error while trigger DAG: Index out of range
        # df = spark.createDataFrame(df.rdd, GH_ARCHIVE_SCHEMA)

        print(f"Cleaning up the data...")
        df = df.drop("payload")
        df = df.drop("other")

        df.show(10)
        df.printSchema()

        outputPath = f"s3a://{bucket}/{folder_name}"
        ti.xcom_push(key="bucket_path", value=outputPath)

        print(f"Writing to Minio at {outputPath}...")
        # Write to Minio
        df\
            .write\
            .format("delta")\
            .mode("overwrite")\
            .option("overwriteSchema", "true")\
            .save(outputPath)
        
        df = spark.read.format("delta").load(outputPath)
        df.printSchema()


@task()
def write_tables_in_gold(ti=None) -> None:
    database = Variable.get("postgres_db")
    user = Variable.get("postgres_user")
    password = Variable.get("postgres_password")

    delta_file_path = ti.xcom_pull(task_ids="load_to_silver", key="bucket_path")
    minio_conf = {
        "endpoint_url": Variable.get("minio_endpoint"),
        "access_key": Variable.get("minio_access_key"),
        "secret_key": Variable.get("minio_secret_key"),
    }

    bucket = Variable.get("minio_bucket")

    with spark_context_manager.get_spark_session({}, "data_lake") as spark:
        print("Spark session created")
        print("*" * 30)
        spark_context_manager.load_minio_config(spark.sparkContext, minio_conf)

        print("set SparkContext")

        print("*" * 30)
        delta_df = spark.read.format("delta").load(delta_file_path)
        delta_df.printSchema()

        users_df = delta_df.select("actor.*").distinct()
        repos_df = delta_df.select("repo.*").distinct()
        events_df = delta_df.select(
            col("actor.id").alias("actor_id"),
            col("org.id").alias("org_id"),
            "id",
            "created_at",
            "type",
            "public",
            col("repo.id").alias("repo_id"),
            col("repo.name").alias("repo_name"),
            col("repo.url").alias("repo_url"),
        )
        orgs_df = delta_df.select("org.*").distinct()
        orgs_df = orgs_df.na.drop()

        users_df\
            .write\
            .format("jdbc")\
            .option("url", f"jdbc:postgresql://postgresql:5432/{database}")\
            .option("driver", "org.postgresql.Driver")\
            .option("dbtable", "users")\
            .option("isolationLevel","NONE")\
            .option("user", f"{user}")\
            .option("password", f"{password}")\
            .mode("append")\
            .save()
        orgs_df\
            .write\
            .format("jdbc")\
            .option("url", f"jdbc:postgresql://postgresql:5432/{database}")\
            .option("driver", "org.postgresql.Driver")\
            .option("dbtable", "organizations")\
            .option("isolationLevel","NONE")\
            .option("user", f"{user}")\
            .option("password", f"{password}")\
            .mode("append")\
            .save()
        repos_df\
            .write\
            .format("jdbc")\
            .option("url", f"jdbc:postgresql://postgresql:5432/{database}")\
            .option("driver", "org.postgresql.Driver")\
            .option("dbtable", "repos")\
            .option("isolationLevel","NONE")\
            .option("user", f"{user}")\
            .option("password", f"{password}")\
            .mode("append")\
            .save()
        events_df\
            .write\
            .format("jdbc")\
            .option("url", f"jdbc:postgresql://postgresql:5432/{database}")\
            .option("driver", "org.postgresql.Driver")\
            .option("dbtable", "events")\
            .option("isolationLevel","NONE")\
            .option("user", f"{user}")\
            .option("password", f"{password}")\
            .mode("append")\
            .save()
        
@task()
def clean_up(ti=None) -> None:
    print("Clean up data files after loading to Minio")
    folder_name = ti.xcom_pull(task_ids="get_file_path", key="folder_name")
    shutil.rmtree(f"{ROOT_DATA_PATH}/{folder_name}", ignore_errors=False, onerror=None)


@dag(
    dag_id="data_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    tags=["load"],
)
def data_pipeline():
    (
        [get_file_path() >> download_to_bronze(), setup_minio()]
        >> load_to_silver()
        >> clean_up()
        >> write_tables_in_gold()
    )


data_pipeline()




