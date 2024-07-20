from collections import namedtuple

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from lib.logger import Log4J
import sys

PlayersInfo = namedtuple("PlayersInfo",["name","team","batting_style","bowling_style","playing_role","description"])
if __name__ == "__main__":
    conf = SparkConf() \
        .setMaster("local[3]") \
        .setAppName("HelloRDD")

    # sc = SparkContext(conf=conf)

    spark = SparkSession \
        .builder \
        .config(conf=conf) \
        .getOrCreate()
    sc = spark.sparkContext
    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: HelloSpark <filename>")
        sys.exit(-1)

    linesRDD = sc.textFile(sys.argv[1])
    partitionedRDD = linesRDD.repartition(2)

    colsRDD = partitionedRDD.map(lambda line: line.split(","))
    selectRDD = colsRDD.map(lambda cols: PlayersInfo(cols[1],cols[2],cols[3],cols[4]))
    filteredRDD = selectRDD.filter(lambda r: r.playing_role == "Batting Allrounder")
    colsList = filteredRDD.collect()
    for x in colsList:
        logger.info(x)