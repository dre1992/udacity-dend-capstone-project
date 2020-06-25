import time
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, DateType, IntegerType, StringType, FloatType, StructType

__author__ = 'dre'


def analyze(spark, input_path, output_path):
    """
    Called by main.py to run the flights etl. Reads the data from s3, transforms them and stores them
    back as parquet files

    @param spark: the session to use
    @param input_path: path of the data to read
    @param output_path: path to write the data
    """

    start = time.time()
    # get filepath to log data file
    data_sparkhema = [StructField('FL_DATE', DateType(), True),
                      StructField('OP_UNIQUE_CARRIER', StringType(), True),
                      StructField('TAIL_NUM', StringType(), True),
                      StructField('ORIGIN_AIRPORT_ID', IntegerType(), True),
                      StructField('ORIGIN_AIRPORT_SEQ_ID', IntegerType(), True),
                      StructField('ORIGIN_CITY_MARKET_ID', IntegerType(), True),
                      StructField('ORIGIN', StringType(), True),
                      StructField('ORIGIN_CITY_NAME', StringType(), True),
                      StructField('ORIGIN_STATE_ABR', StringType(), True),
                      StructField('ORIGIN_STATE_FIPS', StringType(), True),
                      StructField('ORIGIN_STATE_NM', StringType(), True),
                      StructField('ORIGIN_WAC', StringType(), True),
                      StructField('DEST_AIRPORT_ID', IntegerType(), True),
                      StructField('DEST_AIRPORT_SEQ_ID', IntegerType(), True),
                      StructField('DEST_CITY_MARKET_ID', IntegerType(), True),
                      StructField('DEST', StringType(), True),
                      StructField('DEST_CITY_NAME', StringType(), True),
                      StructField('DEST_STATE_ABR', StringType(), True),
                      StructField('DEST_STATE_FIPS', StringType(), True),
                      StructField('DEST_STATE_NM', StringType(), True),
                      StructField('DEST_WAC', StringType(), True),
                      StructField('DEP_TIME', StringType(), True),
                      StructField('DEP_DELAY', FloatType(), True),
                      StructField('DEP_DELAY_NEW', FloatType(), True),
                      StructField('DEP_DEL15', FloatType(), True),
                      StructField('ARR_TIME', StringType(), True),
                      StructField('ARR_DELAY', FloatType(), True),
                      StructField('ARR_DELAY_NEW', FloatType(), True),
                      StructField('ARR_DEL15', FloatType(), True),
                      StructField('CANCELLED', FloatType(), True),
                      StructField('CANCELLATION_CODE', StringType(), True),
                      ]
    spark_schema = StructType(data_sparkhema)
    df = spark.read.csv("{}/flights/flights_*_*.csv".format(input_path), header=True, schema=spark_schema)
    year_month = df.withColumn('YEAR', F.year('FL_DATE')) \
        .withColumn('MONTH', F.month('FL_DATE')).withColumn('CANCELLED', df.CANCELLED.cast(IntegerType()))
    df_lower = year_month.toDF(*[c.lower() for c in year_month.columns])

    df_lower.withColumn("_year", F.col("year")).withColumn("_month", F.col("month")) \
        .dropDuplicates().write.partitionBy(['_year', '_month']) \
        .parquet("{}/flights_parquet/flights.parquet".format(output_path), mode='overwrite')
    end = time.time()
    print("\nExecution of job load covid took %s seconds" % (end - start))


