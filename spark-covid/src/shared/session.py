import configparser
import os

from pyspark.sql import SparkSession


def create_spark_session():
    """
    Creates the spark session

    :return: the spark session
    """

    # We configure spark to download the necessary hadoop-aws dependencies
    # and se the fileoutputcommitter to 2 for better handling of writing data to s3
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:2.7.0",
                ) \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .getOrCreate()
    return spark
