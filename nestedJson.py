from pyspark.sql import *
from pyspark.sql.functions import explode, col

if __name__ == "__main__":
    spark = SparkSession.builder.appName("nestedJson").master("local[3]").getOrCreate()

    #df_json = spark.read.option("multiline","true").json("data/nestedJson.json")
    df_json = spark.read.json("data/nestedJson.json", multiLine=True)
    emp_df = df_json.withColumn("emp", explode(col("employees")))

    df_new = emp_df.withColumn('City', col('emp.address.city'))\
        .withColumn('state', col('emp.address.state'))\
        .withColumn('name', col('emp.name'))\
        .withColumn('age', col('emp.age'))\
        .withColumn('deptName', col('emp.department.name'))\
        .withColumn('skills', explode(col('emp.skills')))

    df_new.select("City", "state", "name", "age", "deptName", "skills").show()


