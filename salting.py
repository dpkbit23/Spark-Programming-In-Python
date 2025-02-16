from logging import Logger


from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F

from lib.logger import *

if __name__ == "__main__":
    spark = SparkSession.builder \
                        .appName("Salting") \
                        .master("local[*]") \
                        .getOrCreate()

    logger = Log4j(spark)

    spark.conf.set("spark.sql.shuffle.partitions", "3")
    spark.conf.get("spark.sql.shuffle.partitions")
    spark.conf.set("spark.sql.adaptive.enabled","false")

    df_uniform = spark.createDataFrame([i for i in range(1000000)], IntegerType())
    #df_uniform.show(10)

    df_uniform.withColumn("partition", F.spark_partition_id()) \
        .groupBy("partition") \
        .count() \
        .orderBy("partition") \
        .show(15, False)

    df0 = spark.createDataFrame([0] *  1000000, IntegerType()).repartition(1)
    df1 = spark.createDataFrame([1] * 20, IntegerType()).repartition(1)
    df2 = spark.createDataFrame([2] * 15, IntegerType()).repartition(1)
    df3 = spark.createDataFrame([3] * 10, IntegerType()).repartition(1)
    df_skew = df0.union(df1).union(df2).union(df3)
    #df_skew.show()

    df_skew.withColumn("partition", F.spark_partition_id()) \
        .groupBy("partition") \
        .count() \
        .orderBy("partition") \
        .show(15, False)

    df_joined = df_skew.join(df_uniform, 'value', 'inner')

    df_joined.withColumn("partition", F.spark_partition_id()) \
        .groupBy("partition") \
        .count() \
        .orderBy("partition") \
        .show(15, False)

    salt_number  = spark.conf.get("spark.sql.shuffle.partitions")
    print (salt_number)

    df_skew.withColumn("salt", (F.rand() * salt_number).cast("int"))\
                    .groupBy("value", "salt")\
                    .agg(F.count("value").alias("count"))\
                    .groupBy("value")\
                    .agg(F.sum("count").alias("count"))\
                    .show(15, False)
    #df_skew.show()
    #df_skew.createOrReplaceTempView("skewtbl")
    #df_test = spark.sql("select distinct salt from skewtbl")
    #df_test.show()

    #df_skew.groupBy("value").count().show(15, False)

    input("enter any key to continue")