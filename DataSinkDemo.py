from logging import Logger

from pyspark.sql import *
from pyspark.sql.functions import spark_partition_id

#from pyspark.sql.connect.functions import spark_partition_id

from lib.logger import *

if __name__ == "__main__":
    spark = SparkSession \
                .builder \
                .appName("DataSinkDemo") \
                .master("local[2]") \
                .getOrCreate()

    logger = Log4j(spark)

    #flightTimeParquetDF = spark.read.format("parquet").load("data/flight-time.parquet")
    flightTimeParquetDF = spark.read.parquet("data/flight-time.parquet")
    #flightTimeParquetDF.show(5)


    logger.info("No. of partition before : " + str(flightTimeParquetDF.rdd.getNumPartitions()))
    flightTimeParquetDF.groupBy(spark_partition_id()).count().show()

    partitionedDF = flightTimeParquetDF.repartition(5)
    logger.info("No of partition after : " + str(partitionedDF.rdd.getNumPartitions()))
    partitionedDF.groupBy(spark_partition_id()).count().show()

    #partitionedDF.write.format("avro").mode("overwrite").save("Output/avro/")
    flightTimeParquetDF.write.format("json").mode("overwrite").option("path", "Output/json") \
                                .partitionBy("OP_CARRIER","ORIGIN") \
                                .option("maxRecordsPerFile", 10000) \
                                .save()


