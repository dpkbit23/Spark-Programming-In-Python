from pyspark.sql import SparkSession
from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder.appName("ReadMode").master("local[3]").enableHiveSupport().getOrCreate()
    logger = Log4j(spark)
    try :
        df_csvFile = spark.read.csv("data/test.csv", header=True, inferSchema=False, mode="failfast")
        df_csvFile.printSchema()
        #df_csvFile.show()
    except Exception as e :
        print("Error reading csv file : ", e)