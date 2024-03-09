from contextlib import contextmanager
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import delta

def load_spark_config(config):
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

    for k, v in config.items():
        spark_minio_conf[k] = v

    return spark_minio_conf

def load_minio_config(spark_context, minio_cfg):
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.access.key", minio_cfg.get("access_key"))
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.secret.key", minio_cfg.get("secret_key"))
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.endpoint", minio_cfg.get("endpoint_url"))
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.attempts.maximum", "1")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.establish.timeout", "5000")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.timeout", "10000")

@contextmanager
def get_spark_session(config, run_id="Spark IO Manager", spark_master_url = "local[*]"):
    try:
        conf = SparkConf()
        conf.set("spark.cores.max", "4")
        conf.set("spark.executor.cores", "2")

        #  java.lang.NullPointerException: Cannot invoke "org.apache.spark.SparkEnv.conf()" because the return value of "org.apache.spark.SparkEnv$.get()" is null
        # conf.set("spark.driver.bindAddress", "localhost")
        # conf.set("spark.ui.port", "4050")

        spark_minio_conf = load_spark_config(config)

        for k, v in spark_minio_conf.items():
            conf.set(k, v)
        
        builder = (
            SparkSession.builder
                .master(spark_master_url)
                .appName(run_id)
                .config(conf=conf)
            )
        
        # Allow spark to work with delta lake format
        spark = delta.configure_spark_with_delta_pip(builder).getOrCreate()

        yield spark
    except Exception as e:
        raise Exception(f"Error while creating spark session: {e}")   
        
        
