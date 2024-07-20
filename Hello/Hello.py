from pyspark.sql import *

if __name__ == "__main__":
    # print("Hello")

    spark = SparkSession.builder \
        .appName("Hello Spark") \
        .master("local[2]") \
        .getOrCreate()

    data_list = [("Adeeb", 23),
                 ("Somen", 27),
                 ("Babsid", 23)]

    df = spark.createDataFrame(data_list).toDF("Name", "Age")

    df.show()
