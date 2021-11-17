import os
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, \
    StringType
import time
from datetime import datetime, timedelta
from pyspark.sql.functions import col
from pyspark.sql.functions import length


topic_output = "cleaned_data_topic"

def main():

    spark = SparkSession \
        .builder \
        .appName("StreamJsonToKafka") \
        .getOrCreate()

    df_with_schema = read_from_json(spark)
    write_to_kafka(df_with_schema)


def read_from_json(spark):
    schema = StructType([
        StructField("ts", IntegerType(), False),
        StructField("uid", StringType(), False),
    ])

    df_with_schema = spark.read.schema(schema) \
        .json("stream.jsonl")

    return df_with_schema


def write_to_kafka(df_with_schema):
    terminate = datetime.now() + timedelta(seconds=30)
    options_write = {
        "kafka.bootstrap.servers":
            "localhost:9092",
        "topic":
            topic_output,        
    }

    df_cleared = df_with_schema.filter(col("ts").rlike("^[0-9]*$"))    
    df_cleared = df_with_schema.where(length(col("uid")) == 19)
    df_cleared = df_with_schema.filter(col("uid").rlike("^[a-z0-9]*$")) 

    while datetime.now() < terminate:
        df_cleared \
            .selectExpr("CAST(uid AS STRING) AS key",
                        "to_json(struct(*)) AS value") \
            .write \
            .format("kafka") \
            .options(**options_write) \
            .save()
    



if __name__ == "__main__":
    main()