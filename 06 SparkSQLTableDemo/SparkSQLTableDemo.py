from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id
from pyspark.sql.types import *

from lib.logger import Log4J

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkSchemaDemo") \
        .enableHiveSupport() \
        .getOrCreate()

    logger = Log4J(spark)

    flightParquetDF = spark.read \
        .format("parquet") \
        .load("dataSource/flight.parquet")
    flightParquetDF.show(5)
    logger.info("Json Schema: " + flightParquetDF.schema.simpleString())

    # Save Table in spark default database
    # flightParquetDF.write \
    #     .mode("overwrite") \
    #     .saveAsTable("test_tbl")

    # Save table in AIRLINE_DB
    # flightParquetDF.write \
    #     .mode("overwrite") \
    #     .saveAsTable("AIRLINE_DB.test_tbl")

    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
    spark.catalog.setCurrentDatabase("AIRLINE_DB")
    # flightParquetDF.write \
    #     .mode("overwrite") \
    #     .bucketBy(5,"Year","Month") \
    #     .sortBy("Year","Month") \
    #     .saveAsTable("test_tbl")
    # logger.info(spark.catalog.listTables("AIRLINE_DB"))

