from pyspark.sql import SparkSession, Window
from datetime import datetime, timedelta
from pyspark.sql.functions import col, lit, date_add, row_number, date_format, concat

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[3]").appName("matchFixture").getOrCreate()
    data = [(1, "India"), (2, "Pakistan"), (3, "England"),(4, "Australia")]
    columns = ["Id", "TeamName"]
    df_team = spark.createDataFrame(data, columns)

    today = datetime.today()
    sunday = today + timedelta(days=(6 - today.weekday()))

    df_team_join = df_team.alias("team1").crossJoin(df_team.alias("team2"))
    window_spec = Window.orderBy("team1.Id", "team2.Id")
    df_team_fixture = df_team_join.filter(col("team1.id") < col("team2.id"))
    df_team_fixture_dt = df_team_fixture.select(col("team1.TeamName").alias("Team 1"),col("team2.TeamName").alias("Team 2"),
                                           row_number().over(window_spec).alias("MatchNo")
                            ).withColumn("Match Date", date_add(lit(sunday.strftime('%Y-%m-%d')),(col("MatchNo")-1) * 7))
    df_team_fixture_dt.select(concat("Team 1", lit ("  vs  "), "Team 2").alias("Fixtures"), "Match Date").show(truncate=False)


