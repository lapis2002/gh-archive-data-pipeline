import requests
import gzip
import os
import shutil

from connectors import minio_manager, spark_context_manager
from utils import load_configuration
from schema import GH_ARCHIVE_SCHEMA

from pyspark.sql.functions import col

CFG_FILE = "resources/config.yaml"


def download_data(cfg) -> None:
    year, month, day, hour = (
        cfg["timestamp"]["year"],
        cfg["timestamp"]["month"],
        cfg["timestamp"]["day"],
        cfg["timestamp"]["hour"],
    )
    headers = {"User-Agent": "Mozilla/5.0"}

    # hour is 0-23
    url = f"https://data.gharchive.org/{year}-{month:02d}-{day:02d}-{hour}.json.gz"
    print(f"Extracting data from {url}...")
    data = requests.get(url, headers=headers)
    if data.status_code != 200:
        raise ValueError(f"Failed to extract data from {url}")

    print(f"Data extracted from {url}")

    json_data = gzip.decompress(data.content)
    json_data_decoded = json_data.decode("utf-8")

    folder_name = f"{year}-{month:02d}-{day:02d}"
    folder_path = f"resources/sample_data/{folder_name}"
    isExist = os.path.exists(folder_path)
    if not isExist:
        # Create a new directory because it does not exist
        os.makedirs(folder_path)

    json_file_path = f"{folder_path}/{year}-{month:02d}-{day:02d}-{hour}.json"

    with open(json_file_path, "w") as f:
        f.write(json_data_decoded)

    file_path_config = {
        "json_file_path": json_file_path,
        "folder_path": folder_path,
        "folder_name": folder_name,
    }

    return file_path_config


def get_data(cfg) -> None:
    file_path_config = download_data(cfg)

    json_file_path = file_path_config.get("json_file_path")
    folder_path = file_path_config.get("folder_path")
    folder_name = file_path_config.get("folder_name")

    print("Start creating spark session")

    minio_conf = {
        "endpoint_url": cfg["datalake"].get("endpoint"),
        "access_key": cfg["datalake"].get("access_key"),
        "secret_key": cfg["datalake"].get("secret_key"),
    }

    bucket = cfg["datalake"].get("bucket_name")

    with minio_manager.get_minio_client(minio_conf) as client:
        found = client.bucket_exists(bucket_name=bucket)
        if not found:
            client.make_bucket(bucket_name=bucket)
        else:
            print(f"Bucket {bucket} already exists, skip creating!")

    with spark_context_manager.get_spark_session({}, "data_profiling") as spark:
        print("Spark session created")
        print("*" * 30)
        spark_context_manager.load_minio_config(spark.sparkContext, minio_conf)

        print("set SparkContext")

        #^ Error: Change all Schema fields to nullable
        df = spark\
                .read\
                .schema(GH_ARCHIVE_SCHEMA)\
                .option("timestampNTZFormat", "yyyy-MM-dd'T'HH:mm:ss'Z'")\
                .json(json_file_path)
        

        # df = spark.createDataFrame(df.rdd, GH_ARCHIVE_SCHEMA)

        df = df.drop("payload")
        df = df.drop("other")
        df.show(10)
        df.printSchema()

        # json_df.write\
        #     .format("delta")\
        #     .mode("overwrite")\
        #     .save(f"resources/sample_data/{folder_name}/")

        # json_df = spark.createDataFrame(jsonRdd, GH_ARCHIVE_SCHEMA)
        # json_df.printSchema()
        # json_df = json_df.withColumn("created_at", json_df["created_at"].cast("timestamp"))
        # json_df = json_df.drop("payload")

        outputPath = f"s3a://{bucket}/{folder_name}"

        print(f"Writing to Minio at {outputPath}...")
        # Write to Minio
        df.write\
            .format("delta")\
            .mode("overwrite")\
            .option("overwriteSchema", "true")\
            .save(outputPath)
        
        df = spark.read.format("delta").load(outputPath)
        df.printSchema()
        print("Data loaded to Minio")

        database = "github_archive"
        user = "k6"
        password = "k6"

        delta_df = spark.read.format("delta").load(outputPath)

        users_df = delta_df.select("actor.*").distinct()
        users_df.show   (10)
        users_df\
            .write\
            .format("jdbc")\
            .option("url", f"jdbc:postgresql://localhost:5432/{database}")\
            .option("driver", "org.postgresql.Driver")\
            .option("dbtable", "users")\
            .option("isolationLevel","NONE")\
            .option("user", f"{user}")\
            .option("password", f"{password}")\
            .mode("overwrite")\
            .save()
    
    # clean up the data folder
    print(f"Clean up data files after loading to Minio")
    shutil.rmtree(folder_path, ignore_errors=False, onerror=None)


if __name__ == "__main__":
    # main()
    cfg = load_configuration.load_cfg_file(CFG_FILE)
    get_data(cfg)
