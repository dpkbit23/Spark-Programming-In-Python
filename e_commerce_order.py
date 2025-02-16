from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import year, month, col,sum

if __name__ == "__main__":
    spark = SparkSession.builder.appName("e-commerce-order").master("local[*]").getOrCreate()

    data = [(1,"2025-01-15",100),
            (2, "2025-01-20", 200),
            (3, "2025-02-10", 150),
            (4, "2025-02-25", 300),
            (5, "2025-03-05", 400),
            (6, "2026-05-05", 400),(7, "2026-01-05", 200)]

    columns = ["order_id","order_dt","order_amt"]
    df = spark.createDataFrame(data, columns)
    df = df.withColumn("month", month(col("order_dt"))).withColumn("year", year(col("order_dt")))
    df = df.groupBy("year","month").agg(sum("order_amt").alias("Total_amt")).orderBy(df.year,df.month)

    df.show()