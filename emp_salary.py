from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark =  SparkSession.builder.appName("Employee Salary").master("local[3]").getOrCreate()
    data = [(1, "John Doe", "IT", 80000, "2021-01-15"),
            (2, "Jane Smith", "HR", 70000, "2010-05-22"),
            (3, "Robert Brown", "IT", 85000, "2019-11-03"),
            (4, "Emily Davis", "Finance", 90000, "2022-07-19"),
            (5, "Michael Johnson", "HR", 75000, "2023-03-11"), ]

    columns = ["EmpID", "EmpName", "Dept", "Salary", "DoJ"]

    df = spark.createDataFrame(data, columns)
    df.printSchema()
    empCount_df = df.groupBy("Dept").count()
    empCount_df.show()
    empAvgSal_df = df.groupBy("Dept").agg(avg("Salary").alias("AvgSalary"))
    empAvgSal_df.show()

    empDoJ_df = df.filter((col("Dept") == "HR") & (col("DoJ") > "2020-01-01"))
    empDoJ_df.show()
    empDoJ_df = empDoJ_df.coalesce(1)
    empDoJ_df.write.csv("Output/emp_sal_output.csv",header=True, mode="overwrite")