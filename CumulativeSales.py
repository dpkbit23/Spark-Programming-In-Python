from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *



if __name__ == "__main__":
    spark = SparkSession.builder.appName("CumulativeSales").master("local[3]").getOrCreate()

    data = [("East", "January", 200), ("East", "Feb", 300),
            ("East", "Mar", 250), ("West", "Jan", 400),
            ("West", "Feb", 350), ("West", "Mar", 450),
            ("North", "Jan", 300), ("North", "Feb", 300),
            ("North", "Mar", 270), ("South", "Jan", 500),
            ("South", "Feb", 750), ("South", "Mar", 650),("South", "April", 650)
            ]

    columns = ["Region", "Month", "Sales"]

    df = spark.createDataFrame(data, columns)
    month_map = {
        "Jan": 1, "January": 1,
        "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6, "Jul": 7,
        "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
    }

    @udf(IntegerType())
    def mon_name_to_number(name):
        return month_map.get(name)


    df = df.withColumn("Mon_no", mon_name_to_number(col("Month")))
    #df.show()
    window_spec = Window.partitionBy("Region").orderBy("Mon_no")
    result_df = df.withColumn("Cum_Sales", sum("Sales").over(window_spec))\
                .withColumn("Rank", dense_rank().over(window_spec))
    #result_df.select("Rank","Region","Month","Sales","Cum_Sales").show()

    diff_df = result_df.withColumn("prevMon", lag(col("Sales")).over(window_spec))\
                .withColumn("Diff", col("Sales") - col("prevMon"))
    diff_df = diff_df.select("Rank","Region","Month","Sales","prevMon","Cum_Sales",when(col("Diff").isNull(), "0").otherwise(col("Diff")).alias("DiffSales") )
    diff_df.show()
    diff_df.write.json("Output/CumulativeSales.json", mode="overwrite")

