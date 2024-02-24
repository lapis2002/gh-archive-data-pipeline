from connectors import spark_context_manager
from utils import load_configuration
from pydeequ.profiles import ColumnProfilerRunner
import requests
import gzip
from minio import Minio
from glob import glob
import os
from schema import GH_ARCHIVE_SCHEMA

CFG_FILE = 'resources/config.yaml'
def get_data(cfg) -> None:
    year, month, day, hour = cfg["timestamp"]["year"], cfg["timestamp"]["month"], cfg["timestamp"]["day"], cfg["timestamp"]["hour"]
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
    isExist = os.path.exists(f"resources/sample_data/{folder_name}")
    if not isExist:
    # Create a new directory because it does not exist
        os.makedirs(f"resources/sample_data/{folder_name}")

    json_file_path = f"resources/sample_data/{folder_name}/{year}-{month:02d}-{day:02d}-{hour}.json"

    with open(json_file_path, "w") as f:
        f.write(json_data_decoded)

    print(json_data_decoded[:100], json_data_decoded[-100:])
    print("Start creating spark session")


    spark_minio_conf = {
        "spark.jars": """jars/postgresql-42.6.0.jar, 
                         jars/deequ-2.0.3-spark-3.3.jar, 
                         jars/hadoop-aws-2.8.0.jar, 
                         jars/aws-java-sdk-s3-1.11.93.jar,
                         jars/aws-java-sdk-core-1.11.93.jar""",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    }

    minio_conf = {
        "endpoint_url": cfg["datalake"].get("endpoint"),
        "access_key": cfg["datalake"].get("access_key"),
        "secret_key": cfg["datalake"].get("secret_key"),
    }

    client = Minio(
        endpoint=cfg["datalake"].get("endpoint"),
        access_key=cfg["datalake"].get("access_key"),
        secret_key=cfg["datalake"].get("secret_key"),
        secure=False,
    )

    bucket = cfg["datalake"].get("bucket_name")
    found = client.bucket_exists(bucket_name=bucket)
    if not found:
        client.make_bucket(bucket_name=bucket)
    else:
        print(f'Bucket {bucket} already exists, skip creating!')

    with spark_context_manager.get_spark_session(spark_minio_conf, "data_profiling") as spark:
        print("Spark session created")
        print("*"*30)
        spark_context_manager.load_minio_config(spark.sparkContext, minio_conf)

        print("set SparkContext")

        #^ Error: Change all Schema fields to nullable
        df = spark\
                .read\
                .schema(GH_ARCHIVE_SCHEMA)\
                .option("timestampNTZFormat", "yyyy-MM-dd'T'HH:mm:ss'Z'")\
                .json(json_file_path)
        # jsonRdd = spark.sparkContext.textFile(json_file_path)

        df.show(50)        
        df.printSchema()

        df = spark.createDataFrame(df.rdd, GH_ARCHIVE_SCHEMA) 
        df = df.drop("payload")
        df.show(50)        
        df.printSchema()
        # json_df.write\
        #     .format("delta")\
        #     .mode("overwrite")\
        #     .save(f"resources/sample_data/{folder_name}/")
                

        # json_df = spark.createDataFrame(jsonRdd, GH_ARCHIVE_SCHEMA)
        # json_df.printSchema()
        # json_df = json_df.withColumn("created_at", json_df["created_at"].cast("timestamp"))
        # json_df = json_df.drop("payload")
        # json_df.show(50)
        outputPath = f"s3a://{bucket}/{folder_name}"

        # Write to Minio
        df.write\
            .format("delta")\
            .option("overwriteSchema", "true")\
            .mode("overwrite")\
            .save(outputPath)

        print(f"Data saved to {outputPath}")

if __name__ == "__main__":
    # main()
    CFG_FILE = 'resources/config.yaml'
    cfg = load_configuration.load_cfg_file(CFG_FILE)
    folder_name = f"{cfg["timestamp"]["year"]}-{cfg["timestamp"]["month"]:02d}-{cfg["timestamp"]["day"]:02d}"
    cfg["folder_name"] = folder_name
    cfg["datalake"]["folder_name"] = folder_name

    get_data(cfg)