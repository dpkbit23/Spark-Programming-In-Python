from pandas.core.methods.describe import describe_timestamp_1d
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, window, row_number, dense_rank, rank

if __name__ == "__main__":
    spark = SparkSession.builder.appName("dropDuplicate").master("local[3]").getOrCreate()

    data = [(1, 'login', '2025-02-01 10:00:00'),
            (1, 'view_product', '2025-02-01 10:05:00'), (1, 'login', '2025-02-01 10:30:00'),
            (2, 'purchase', '2025-02-01 11:00:00'), (2, 'login', '2025-02-01 11:15:00'),
            (2, 'view_product', '2025-02-01 11:30:00'), (3, 'login', '2025-02-01 12:00:00'),
            (3, 'login', '2025-02-01 12:05:00')]

    df = spark.createDataFrame(data,["user_id","activity_type","activity_timestamp"])
    df1 = df
    #df1.show()
    df = df.dropDuplicates(["user_id","activity_type"])
    #df.show()
    df2 = df1
    window_spec = Window.partitionBy("user_id","activity_type").orderBy(col("activity_timestamp").desc())
    #row_number
    df_rowNum = df1.withColumn("row_num", row_number().over(window_spec))
    df_filter = df_rowNum.filter(col("row_num") == 1)
    df_filter.show()
    #dense rank
    df_dense_rank = df2.withColumn("rank", dense_rank().over(window_spec))
    df_dense_rank_filter  = df_dense_rank.filter(col("rank")== 1).drop(col("rank"))
    df_dense_rank_filter.show()

    #rank
    df_rank = df2.withColumn("rank", rank().over(window_spec))
    df_rank.show()
    df_rank_filter = df_rank.filter(col("rank")== 1)
    df_rank_filter.show()
