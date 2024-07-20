from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import StructType

from lib.logger import Log4J


def to_date_df(df, fmt, fld):
    return df.withColumn(fld, to_date(col(fld), fmt))


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("RowDemo") \
        .getOrCreate()

    logger = Log4J(spark)
    # StructField("id", StringType(), True),
    mySchema: StructType = StructType([
        StructField("id", StringType(), True),
        StructField("EventDate", StringType(), True)
    ])

    # data = [
    #     ["1", "04/05/2024"],
    #     ["2", "12/12/2024"],
    #     ["3", "15/09/2024"],
    #     ["4", "21/01/2024"]
    # ]

    my_rows = [Row("04/05/2024"), Row("12/12/2024"), Row("15/09/2024"), Row("21/01/2024")]
    my_rdd = spark.sparkContext.parallelize(my_rows, 2)
    # colsList = my_rdd.collect()
    # for x in colsList:
    #     logger.info(x)

    my_df = spark.createDataFrame(my_rdd, mySchema)

    # df = spark.read \
    #     .format("csv") \
    #     .schema(schema=schema) \
    #     .load("dataSource/date.csv")
    # df.show()
    # df.printSchema()
    # new_df = to_date_df(df, "d/M/y", "EventDate")
    # new_df.printSchema()

    # new_df.show()

    # df = spark.read \
    #     .format("csv") \
    #     .option("header","true") \
    #     .option("inferSchema", "true") \
    #     .load("dataSource/Sampledatafile.csv")
    #
    # df.show()
    # df.printSchema()
    # new_df = to_date_df(df, "M/d/y", "EventDate")
    # new_df.printSchema()
