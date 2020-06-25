import time
import pyspark.sql.functions as F

__author__ = 'dre'


def analyze(spark, input_path, output_path):
    """
    Called by main.py to run the covid etl. Reads the data from s3, transforms them and stores them
    back as parquet files

    @param spark: the session to use
    @param input_path: path of the data to read
    @param output_path: path to write the data
    """
    start = time.time()

    # Call the job provided in the arguments
    df = spark.read.json("{}/covid19/download.json".format(input_path), multiLine=True)

    records_exploded = df.select(F.explode("records").alias("records")).cache()
    records_select = records_exploded.selectExpr(
        "records.countriesAndTerritories as country", "records.continentExp as continent",
        "records.geoId", "cast(records.cases as int)",
        "cast(records.deaths as int)", "records.dateRep", "cast(records.day as int)", "cast(records.month as int)",
        "cast(records.year as int)")
    records_final = records_select.withColumn("dateRep", F.to_date("dateRep", "dd/MM/yyyy"))\
        .withColumn("_year", F.col("year")).withColumn("_month", F.col("month"))

    records_final_lower = records_final.toDF(*[c.lower() for c in records_final.columns])
    records_final_lower.printSchema()
    records_final_lower.dropDuplicates().repartition(F.col("continent")).write.partitionBy(['_year', '_month']) \
        .parquet("{}/covid_parquet/covid.parquet".format(output_path), mode='overwrite')

    end = time.time()

    print("\nExecution of job load covid took %s seconds" % (end - start))


