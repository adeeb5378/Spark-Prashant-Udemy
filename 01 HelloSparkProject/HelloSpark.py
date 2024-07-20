import sys

from pyspark.sql import *
from lib.logger import Log4J
from pyspark import SparkConf

from lib.utils import get_spark_app_config, load_cricket_df,greaterSR,countNoOfPlayingRole

if __name__ == "__main__":
    # conf = SparkConf()
    # conf.set("spark.app.name","Hello Spark")
    # conf.set("spark.master","local[3]")

    conf = get_spark_app_config()
    spark = SparkSession\
        .builder\
        .config(conf=conf) \
        .getOrCreate()

        # .appName("Hello Spark")\
        # .master("local[3]")\


    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: HelloSpark <filename>")
        sys.exit(-1)


    logger.info("Starting HelloSpark")

    # conf_out = spark.sparkContext.getConf()
    # logger.info(conf_out.toDebugString())

    # df = spark.read \
    #     .option("header","true") \
    #     .option("inferSchema","true") \
    #     .csv(sys.argv[1])

    df = load_cricket_df(spark,sys.argv[1])
    partitioned_df = df.repartition(2)
    # df1 = greaterSR(partitioned_df)
    # logger.info(df1.collect())

    # result = partitioned_df.count()
    # print(result)

    query_df = countNoOfPlayingRole(partitioned_df)
    logger.info(query_df.collect())
    query_df.show()

    logger.info("Finished HelloSpark")

    input("Press Enter!")

    spark.stop()


