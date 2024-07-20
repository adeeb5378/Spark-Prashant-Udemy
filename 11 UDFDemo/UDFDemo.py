import re

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import StructType

from lib.logger import Log4J


def parse_gender(gender):
    female_pattern = r"^f$|f.m|w.m"
    male_pattern = r"^m$|ma|m.l"
    if re.search(female_pattern, gender.lower()):
        return "Female"
    elif re.search(male_pattern, gender.lower()):
        return "Male"
    else:
        return "Unknown"


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("RowDemo") \
        .getOrCreate()
    logger = Log4J(spark)
    df = spark.read \
        .format("csv") \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .load("dataSource/survey.csv")

    # df.show()
    parse_gender_udf = udf(parse_gender, StringType())
    df2 = df.withColumn("Gender", parse_gender_udf("Gender"))
    [logger.info(f) for f in spark.catalog.listFunctions() if "parse_gender" in f.name]
    logger.info("Before")

    spark.udf.register("parse_gender_udf",parse_gender,StringType())
    df3 = df.withColumn("Gender", expr("parse_gender_udf(Gender)"))
    df3.show()
    logger.info("After")
    [logger.info(f) for f in spark.catalog.listFunctions() if "parse_gender" in f.name]
    # y = spark.catalog.listFunctions()
    # print(y)

