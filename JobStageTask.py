import sys
from pyspark.sql import *
from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("JobStageTask") \
        .master("local[2]") \
        .getOrCreate()

    logger = Log4j(spark)

    #if len(sys.argv) != 2:
       # logger.error("Usage: JobStageTask <filename>")
       # sys.exit(-1)

    logger.info("Starting JobStageTask")
    #sample_df = spark.read.csv('/Users/nitikakumari/Documents/WorkingProjects/Data/test.csv', header=True, inferSchema=True)
    sample_df = spark.read.option("header", "true")\
    .option("inferSchema", "true") .csv("/Users/nitikakumari/Documents/WorkingProjects/Data/test.csv")
    #sample_df.show()
    partitioned_sample_df = sample_df.repartition(2)
    count_df = partitioned_sample_df.filter("Age > 40") \
    .select("Age","Gender","Country").groupBy("Country")\
    .count()

    logger.info(count_df.collect())

    input("Press Enter to continue...")

    spark.stop()
    #count_df.show()