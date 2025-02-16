
from pyspark.sql import *
from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder \
            .master("local[3]") \
            .appName("SparkSQLHiveTable") \
            .enableHiveSupport() \
            .getOrCreate()

    logger = Log4j(spark)

    flightTimeParquetDF = spark.read.format("parquet").load("data/flight-time.parquet")
    #flightTimeParquetDF.show()
    spark.sql("CREATE DATABASE if not exists AIRLINE_DB")
    spark.catalog.setCurrentDatabase("AIRLINE_DB")

    '''
    flightTimeParquetDF.write \
            .format("csv") \
            .mode("overwrite") \
            #.partitionBy("ORIGIN","OP_CARRIER") \
            .bucket(5,"ORIGIN","OP_CARRIER") \
            .saveAsTable("flight_data_tbl")
    '''
    flightTimeParquetDF.write \
        .format("csv") \
        .mode("overwrite") \
        .bucketBy(5, "ORIGIN", "OP_CARRIER") \
        .sortBy("ORIGIN", "OP_CARRIER") \
        .saveAsTable("flight_data_tbl")

    spark.sql("Create  table if not exists abc (Id INT, Name STRING)")
    result = spark.sql("select count(*) from flight_data_tbl")
    result.show()
    logger.info(spark.catalog.listTables("AIRLINE_DB"))
    logger.info(spark.catalog.getTable("flight_data_tbl"))