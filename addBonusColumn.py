from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, round, dense_rank

if __name__ == "__main__":
    spark = SparkSession.builder.appName("addBonusCol").master("local[3]").getOrCreate()

    data = [(1, "John Doe", "IT", 80000, "2021-01-15"),
            (2, "Jane Smith", "HR", 70000, "2010-05-22"),
            (3, "Robert Brown", "IT", 85000, "2019-11-03"),
            (4, "Emily Davis", "Finance", 90000, "2022-07-19"),
            (5, "Michael Johnson", "HR", 75000, "2023-03-11"),
            (6, "Michael Johnson", "HR", 76000, "2023-03-11")]

    columns = ["EmpID", "EmpName", "Dept", "Salary", "DoJ"]

    df_sal = spark.createDataFrame(data, columns)
    df_bonus = df_sal.withColumn("bonus", round(col("Salary")* 0.15,3))
    df_bonus.show()

    window_spec = Window.partitionBy(col("Dept")).orderBy(col("Salary").desc())

    df_second_sal = df_sal.withColumn("rank", dense_rank().over(window_spec))
    df_second_sal.show()
    df_second_sal.filter((col("rank") == 2) | (col("rank") == 1)).show()
    '''df = df_second_sal.filter((col("rank") == 2) |
           ((col("rank") == 1) & (~df_second_sal.Dept.isin(
               df_second_sal.filter(col("rank") == 2).select(col("Dept")).rdd.flatMap(
                   lambda x: x)
           ))))
    #df_sal.show()
    df.show()
    
   
    # Sample Data (Employee, Department, Salary)
    data = [
        ("Alice", "HR", 5000),
        ("Bob", "HR", 7000),
        ("Charlie", "IT", 6000),
        ("David", "IT", 8000),
        ("Eve", "IT", 8000),  # Duplicate highest salary
        ("Frank", "Sales", 4000)  # Only one employee in Sales
    ]

    columns = ["Name", "Department", "Salary"]

    # Create DataFrame
    df = spark.createDataFrame(data, columns)

    # Define Window Spec (Partition by Department, Order by Salary Desc)
    windowSpec = Window.partitionBy("Department").orderBy(col("Salary").desc())

    # Rank Salaries within Each Department
    df_ranked = df.withColumn("rank", dense_rank().over(windowSpec))

    # Filter for second highest (rank=2), or highest if rank=1 and no rank=2 exists
    df_second_highest = df_ranked.filter((col("rank") == 2) |
                                         ((col("rank") == 1) & (~df_ranked.Department.isin(
                                             df_ranked.filter(col("rank") == 2).select("Department").rdd.flatMap(
                                                 lambda x: x).collect()
                                         ))))

    # Show Result
    df_second_highest.select("Name", "Department", "Salary").show()
    '''