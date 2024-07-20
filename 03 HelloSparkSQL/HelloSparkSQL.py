import sys

from pyspark.sql import *
from lib.logger import Log4J
from pyspark import SparkConf

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("HelloSparkSQL") \
        .getOrCreate()

    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: HelloSpark <filename>")
        sys.exit(-1)

    logger.info("Starting HelloSparkSQL")

    players_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(sys.argv[1])

    # logger.info(players_df.collect())

    players_df.createOrReplaceTempView("playersTbl")
    countDF = spark.sql("SELECT playingRole,count(*) as NoOfPlayers from playersTbl group by playingRole")
    # logger.info(countDF.collect())
    countDF.show()


    logger.info("Finished HelloSparkSQL")

    spark.stop()
