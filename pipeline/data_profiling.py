from connectors import spark_context_manager
from utils import load_configuration
from pydeequ.profiles import ColumnProfilerRunner

CFG_FILE = 'resources/config.yaml'

def main():
    cfg = load_configuration.load_cfg_file(CFG_FILE)
    datalake_cfg = cfg["datalake"]
    
    config = {
        "endpoint_url": datalake_cfg.get("endpoint"),
        "minio_access_key": datalake_cfg.get("access_key"),
        "minio_secret_key": datalake_cfg.get("secret_key"),
    }

    print("Start creating spark session")


    spark_minio_conf = {
        "spark.jars": "jars/postgresql-42.6.0.jar, jars/deequ-2.0.3-spark-3.3.jar, jars/hadoop-aws-2.8.0.jar, jars/aws-java-sdk-s3-1.9.39.jar,jars/aws-java-sdk-core-1.9.37.jar",
        'spark.hadoop.fs.s3a.aws.credentials.provider': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider'
    }

    with spark_context_manager.get_spark_session(spark_minio_conf, "data_profiling") as spark:
        print("Spark session created")
        print("*"*30)
        spark_context_manager.load_minio_config(spark.sparkContext, datalake_cfg)

        print("set SparkContext")
        
        # Load data
        parquet_file = "fake_data.parquet"
        inputPath = f"s3a://{datalake_cfg.get('bucket_name')}/{datalake_cfg.get('folder_name')}/{datalake_cfg.get('file_name')}"

        # Read from a parquet file (transformation)
        # https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html

        df = spark.read.parquet(inputPath)
        df.show(5)
        # Profile data with Deequ
        # https://aws.amazon.com/blogs/big-data/test-data-quality-at-scale-with-deequ/
        # https://pydeequ.readthedocs.io/en/latest/README.html
        result = ColumnProfilerRunner(spark).onData(df).run()

        for col, profile in result.profiles.items():
            if col == "index":
                continue

            print("*" * 30)
            print(f"Column: {col}")
            print(profile)
        
        outputPath = f"s3a://{datalake_cfg.get('bucket_name')}/{'profile_' + datalake_cfg.get('folder_name')}"

        # Write back to Minio
        df.write.mode("overwrite").parquet(outputPath)

if __name__ == "__main__":
    main()