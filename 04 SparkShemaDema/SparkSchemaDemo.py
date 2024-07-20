from pyspark.sql import SparkSession
from pyspark.sql.types import *

from lib.logger import Log4J

if __name__ == "__main__":
    spark = SparkSession \
            .builder \
            .master("local[3]") \
            .appName("SparkSchemaDemo") \
            .getOrCreate()

    logger = Log4J(spark)

    schemaStruct = StructType([
        StructField("year",IntegerType(),True),
        StructField("month",StringType(),True),
        StructField("passengers",IntegerType(),True)
    ])

    schemaDDL = "year INT,month STRING,passengers INT"
    flightCsvDF = spark.read \
        .format("csv") \
        .option("header","true") \
        .schema(schema=schemaDDL) \
        .load("data/flights.csv")
        # .option("inferSchema","true") \

    flightCsvDF.show()
    logger.info("CSV Schema: " + flightCsvDF.schema.simpleString())

    # flightJsonDF = spark.read \
    #     .format("json") \
    #     .load("data/flights_json.json")
    # flightJsonDF.show(5)
    # logger.info("Json Schema: "+ flightJsonDF.schema.simpleString())

    # flightParquetDF = spark.read \
    #     .format("parquet") \
    #     .load("data/flight.parquet")
    # flightParquetDF.show(5)
    # logger.info("Json Schema: " + flightParquetDF.schema.simpleString())