# from pyspark.sql import *
#
# if __name__ == "__main__":
#     print("Hello Spark")
#
#     spark = SparkSession.builder.appName("Hello Spark").master("local[2]").getOrCreate()
#
#     data_list = [("Adeeb", 23),
#                  ("Some", 27),
#                  ("BadSid", 23)]
#
#     df = spark.createDataFrame(data_list).toDF("Name", "Age")
#     df.show()


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create a Spark session
spark = SparkSession.builder.appName("TupleDataFrame").getOrCreate()

# Define the schema for the DataFrame
schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True)
])

# Sample data as a list of tuples
data = [("John", 25), ("Alice", 30), ("Bob", 22)]

# Create a DataFrame from the list of tuples and schema
df = spark.createDataFrame(data, schema=schema)

# Show the DataFrame
df.show()

# Stop the Spark session
spark.stop()
