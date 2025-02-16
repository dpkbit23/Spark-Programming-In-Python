from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, to_timestamp, datediff, current_date, floor, year, month, date_format, \
    date_add, dayofweek, expr

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[3]").appName("date_related").getOrCreate()

    data =  [("A","2024-02-15 23:23:23","1985-08-17"),("B","2023-01-23 14:30:00","2024-02-14")]
    column = ["name","doj","dob"]
    df = spark.createDataFrame(data, column)
    df.printSchema()
    #convert a string column to date format
    df_date = (df.withColumn("dateTime", to_timestamp(col("doj"), "yyyy-MM-dd HH:mm:ss"))
               .withColumn("date", to_date(col("dob"), "yyyy-MM-dd")))
    df_date.printSchema()
    df_date.show()

    #calculate age from the date of birth
    df_age = (df.withColumn("age", floor(datediff(current_date(),col("dob"))/ 365.25)).withColumn("days", datediff(current_date(),col("dob"))))
    df_age.show()

    df_born2000 = df.withColumn("Year", year(col("dob"))).filter(col("Year") > "2000")
    df_born2000.show()

    #extract month and year from DOB
    df_yr_mnth = (df.withColumn("year", year(col("dob"))).withColumn("month", month(col("dob")))
                  .withColumn("monthName", date_format(col("dob"),"MMMM")))
    df_yr_mnth.show()

    df_date_add = df.withColumn("date_30", date_add(col("dob"),30))
    df_date_add.show()

    df_day_week = (df.withColumn("dayOfWeek", dayofweek(col("dob")))
                   .withColumn("dayName", date_format(col("dob"), "EEEE")))
    df_day_week.show()

    df_leapYr = df_yr_mnth.withColumn("leapYr", expr("CASE WHEN year % 4 == 0 THEN 'Leap' else 'Not leap' end"))
    df_leapYr.show()
