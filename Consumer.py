import os
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import NullType, StructField, StructType, IntegerType, \
    StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import col,row_number 

topic_input = "cleaned_data_topic"

def main():
    
    spark = SparkSession \
        .builder \
        .appName("Consumer") \
        .getOrCreate()

    df_with_schema = read_from_kafka(spark)
    print(summarize_sales(df_with_schema))

def read_from_kafka(spark):
    options_read = {
        "kafka.bootstrap.servers":
            "localhost:9092",
        "subscribe":
            topic_input,
        "startingOffsets":
            "earliest",
        "endingOffsets":
            "latest",        
    }

    df_with_schema = spark.read \
        .format("kafka") \
        .options(**options_read) \
        .load()

    return df_with_schema

def summarize_sales(df_with_schema):
    schema = StructType([
        StructField("ts", IntegerType(), False),
        StructField("uid", StringType(), False),     
    ])

    window = Window.partitionBy("ts").orderBy("uid")
    window_agg = Window.partitionBy("ts")    

    df_output = df_with_schema \
        .selectExpr("CAST(value AS STRING)") \
        .select(F.from_json("value", schema=schema).alias("data")) \
        .select("data.*") \
        .withColumn("row", F.row_number().over(window)) \
        .withColumn('count', F.size(F.collect_set("uid").over(window_agg)))\
        .where(F.col("row") == 1).drop("row") \
        .where(col("ts").isNotNull()) \
        .select("ts", "count")
    
    df_output \
        .write \
        .format("console") \
        .option("truncate", False) \
        .save()
        
if __name__ == "__main__":
    main()