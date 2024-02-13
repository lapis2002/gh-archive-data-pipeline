from contextlib import contextmanager
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

def load_minio_config(spark_context, minio_cfg):
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.access.key", minio_cfg.get("access_key"))
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.secret.key", minio_cfg.get("secret_key"))
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.endpoint", minio_cfg.get("endpoint"))
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

        for k, v in config.items():
            conf.set(k, v)
        spark = (
            SparkSession.builder.master(spark_master_url)
            .appName(run_id)
            .config(conf=conf)
            .getOrCreate()
        )
        yield spark
    except Exception as e:
        raise Exception(f"Error while creating spark session: {e}")   
        
        
