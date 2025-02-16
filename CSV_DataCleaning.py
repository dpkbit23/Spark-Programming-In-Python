from pyspark.sql import SparkSession
from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Data Cleaning").master("local[3]").enableHiveSupport().getOrCreate()

    logger = Log4j(spark)

    df_csv = spark.read.csv("data/test.csv", header=True, inferSchema=True)

    filled_values = {
        "Age" : 0,
        "self_employed" : "NAA",
        "family_history" : "Noo",
        "Country": "unknown"
    }
    #df_csv.printSchema()
    #df_csv.show()
    #df_clean = df_csv.dropna(how="any")
    df_clean = df_csv.fillna(filled_values)
    df_clean.createOrReplaceTempView("temp_view")
    #df_csv.na.drop("any").show()
    #df_clean.show()
    #spark.sql("CREATE DATABASE if not exists TEST_DB")
    #spark.catalog.setCurrentDatabase("TEST_DB")
    #logger.info(spark.catalog.listTables("TEST_DB"))
    #spark.sql(f"CREATE TABLE if not exists TEST_DB.CLEANCSV ({', '.join([f'{field.name} {field.dataType.simpleString()}' for field in df_clean.schema])})")
    #spark.sql("Insert into TEST_DB.CLEANCSV select * from temp_view")
    df_tblData = spark.sql("select * from temp_view")
    #spark.sql("DESCRIBE TABLE TEST_DB.CLEANCSV").show()
    #spark.sql("DROP TABLE TEST_DB.CLEANCSV")
    df_tblData.write.mode("overwrite").saveAsTable("TEST_DB.CLEANCSV")
    spark.sql("select  * from TEST_DB.CLEANCSV").show()










    '''
    input_data = [(1, "Shivansh", "Data Scientist", "Noida"),
                  (2, None, "Software Developer", None),
                  (3, "Swati", "Data Analyst", "Hyderabad"),
                  (4, None, None, "Noida"),
                  (5, "Arpit", "Android Developer", "Banglore"),
                  (6, "Ritik", None, None),
                  (None, None, None, None)]
    schema = ["Id", "Name", "Job Profile", "City"]

    # calling function to create dataframe
    df = spark.createDataFrame(input_data, schema)
    #df.write.csv("data/NullCSV")
    #df.dropna("any").show()
    '''