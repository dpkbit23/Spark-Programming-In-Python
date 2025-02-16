from pyspark.sql import functions as F
from pyspark.sql import SparkSession, Window

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[3]").appName("Window_funct").getOrCreate()

    summary_df = spark.read.parquet("data/summary.parquet")

    '''Window aggregation'''
    running_total_window = Window.partitionBy("Country")\
                            .orderBy("WeekNumber")\
                            .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    summary_df = summary_df.withColumn("total_window", F.sum("InvoiceValue").over(running_total_window))
    #summary_df.show()

    '''LAG and LEAD'''
    summary_df.createOrReplaceTempView("summaryView")
    View_df = spark.sql("select *,LAG(InvoiceValue)  over( partition by (Country) order by (WeekNumber) )  as lagCOl ,"
                        "coalesce(InvoiceValue- LAG(InvoiceValue)  over( partition by (Country) order by (WeekNumber) ) ,0) as Invoice_diff"
                        " from summaryView")
    View_df.show()