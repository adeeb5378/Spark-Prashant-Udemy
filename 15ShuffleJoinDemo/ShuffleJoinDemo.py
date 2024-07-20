import re
import sys
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import StructType

from lib.logger import Log4J

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local") \
        .appName("ShuffleJoin") \
        .enableHiveSupport() \
        .getOrCreate()

    flightDF1 = spark.read.json("data/d1/")
    # print(flightDF1.rdd.getNumPartitions())
    # flightDF1.show()
    # print(flightDF1.count())
    flightDF2 = spark.read.json("data/d2/")
    # print(flightDF2.rdd.getNumPartitions())
    # flightDF2.show()
    # print(flightDF2.count())

    # spark.conf.set("spark.sql.shuffle.partitions", 3)
    # join_expr = flightDF1.id == flightDF2.id
    # shuffle_join_df = flightDF1.join(flightDF2, join_expr, "inner")
    # broadcast_join_df = flightDF1.join(broadcast(flightDF2), join_expr, "inner")
    # shuffle_join_df.show()
    # broadcast_join_df.show()

    # Join with Bucketing
    spark.sql("SHOW DATABASES")
    # flightDF1.repartition(1).write \
    #     .bucketBy(3, "id") \
    #     .sortBy("id") \
    #     .mode("overwrite") \
    #     .save("flight_data1")

