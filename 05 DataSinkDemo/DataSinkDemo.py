from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id
from pyspark.sql.types import *

from lib.logger import Log4J

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkSchemaDemo") \
        .getOrCreate()

    logger = Log4J(spark)

    flightParquetDF = spark.read \
        .format("parquet") \
        .load("dataSource/flight.parquet")
    flightParquetDF.show(5)
    logger.info("Json Schema: " + flightParquetDF.schema.simpleString())

    # logger.info("Number of Partitions before : " + str(flightParquetDF.rdd.getNumPartitions()))
    #
    # # No of records in each partitions
    # flightParquetDF.groupBy(spark_partition_id()).count().show()
    #
    # partitionedDF = flightParquetDF.repartition(5)
    #
    # logger.info("Number of Partitions After : " + str(partitionedDF.rdd.getNumPartitions()))
    # partitionedDF.groupBy(spark_partition_id()).count().show()

    # flightParquetDF.write \
    #     .format("avro") \
    #     .mode("overwrite") \
    #     .save("dataSink/avro")

    # flightParquetDF.write \
    #     .format("json") \
    #     .mode("append") \
    #     .option("path", "dataSink/json/") \
    #      .partitionBy("Year") \
    #      .option("maxRecordsPerFile",1000)
    #     .save()

    # flightParquetDF.repartition(3).write \
    #     .format("json") \
    #     .mode("append") \
    #     .option("path", "dataSink/json/") \
    #     .save()





    # yearDF = flightParquetDF.rdd.partitionBy("Year")
    # logger.info("Number of Years : " + str(yearDF.rdd.getNumPartitions()))

    # flightParquetDF.write.partitionBy("Year") \
    #     .format("json") \
    #     .mode("overwrite") \
    #     .save("dataSink")


