import re

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import StructType

from lib.logger import Log4J

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("RowDemo") \
        .getOrCreate()

    spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
    logger = Log4J(spark)

    # mySchema = StructType([
    #     StructField("InvoiceNo", IntegerType(), False),
    #     StructField("StockCode", IntegerType(), False),
    #     StructField("Description", StringType(), False),
    #     StructField("Quantity", IntegerType(), False),
    #     StructField("InvoiceDate", TimestampType(), False),
    #     StructField("UnitPrice", FloatType(), False),
    #     StructField("CustomerID", IntegerType(), False),
    #     StructField("Country", StringType(), False)
    # ])

    df = spark.read \
        .format("csv") \
        .option("nullValue","null")\
        .option("header","true") \
        .option("inferSchema","true")\
        .load("dataSource/invoices.csv")

    # df.show()
    # df.printSchema()

    # df.select(count("*").alias("Count *"),
    #           sum("Quantity").alias("TotalQuantity"),
    #           avg("UnitPrice").alias("AvgPrice"),
    #           countDistinct("InvoiceNo").alias("CountDistinct")
    #           ).show()

    # df.selectExpr(
    #     "count(1) as `countFiled`",
    #     "count(StockCode) as `NoOfStocks`",
    #     "sum(Quantity) as `TotalQuantity`"
    # ).show()

    df.createOrReplaceTempView("sales")
    summary_sql = spark.sql("""
    SELECT Country,InvoiceNo,
    sum(Quantity) as TotalQuantity,
    round(sum(Quantity*UnitPrice),2) as InvoiceValue
    FROM sales
    GROUP BY Country,InvoiceNo
    """)
    # summary_sql.show()

    summary_df = df \
        .groupBy("Country", "InvoiceNo") \
        .agg(sum("Quantity").alias("TotalQuantity"),
             round(sum(expr("Quantity*UnitPrice")), 2).alias("InvoiceValue")
             )

    # summary_df.show()

    exSummary_df = df\
        .withColumn("InvoiceDate",to_date(col("InvoiceDate"),'dd-MM-yyyy HH.mm'))\
        .withColumn("weekNumber",weekofyear(col("InvoiceDate")))
    # exSummary_df.show()

    exSummary_df1 = exSummary_df.groupBy("Country","weekNumber").agg(
        sum("Quantity").alias("TotalQuantity"),
        countDistinct("InvoiceNo").alias("NumOfInvoices"),
        round(sum(expr("Quantity*UnitPrice")), 2).alias("InvoiceValue")
    )

    # exSummary_df1.coalesce(1)\
    #     .write\
    #     .format("parquet")\
    #     .mode("overwrite")\
    #     .save("outputSource/")
    # exSummary_df1.sort("Country","weekNumber").show()

    running_total_window = Window.partitionBy("Country")\
        .orderBy("weekNumber") \
        .rowsBetween(Window.unboundedPreceding,Window.currentRow)

    exSummary_df1.withColumn("RunningTotal",
                             sum("InvoiceValue").over(running_total_window)).show()
