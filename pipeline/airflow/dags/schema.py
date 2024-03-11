from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampNTZType, BooleanType
from pyspark.sql import functions as F

GH_ARCHIVE_SCHEMA = StructType(
    [
        StructField("id", StringType(), nullable=False),
        StructField("other", StringType()),
        StructField("created_at", StringType(), nullable=False),
        StructField("type", StringType(), nullable=False),
        StructField("public", BooleanType(), nullable=False),
        StructField("repo", StructType([
            StructField("id", LongType(), nullable=False),
            StructField("name", StringType(), nullable=False),
            StructField("url", StringType(), nullable=False),
        ]), nullable=False),
        StructField("actor", StructType([
            StructField("id", LongType(), nullable=False),
            StructField("login", StringType(), nullable=False),
            StructField("gravatar_id", StringType()),
            StructField("avatar_url", StringType()),
            StructField("url", StringType(), nullable=False),
        ]), nullable=False),
        StructField("org", StructType([
            StructField("id", LongType(), nullable=False),
            StructField("login", StringType(), nullable=False),
            StructField("gravatar_id", StringType()),
            StructField("avatar_url", StringType()),
            StructField("url", StringType(), nullable=False),
        ])),
        StructField("payload", StringType()),

    ]
)