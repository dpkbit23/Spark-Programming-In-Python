from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, desc

if __name__ == "__main__":
    spark = SparkSession.builder.appName("wordCount").master("local[3]").getOrCreate()

    #using data frame
    df = spark.read.text("data/To-Do topics for study")

    word_count = (
        df.select(explode(split(col("value")," ")).alias("word"))
        .groupBy("word")
        .count()
    )
    word_count.show()
    high_word_count = word_count.orderBy(desc("count")).limit(1)
    high_word_count.show()

    #using RDD
    rdd = spark.sparkContext.textFile("data/To-Do topics for study")
    rdd_word_count = (
        rdd.flatMap(lambda line: line.split(" "))
        .map(lambda word: (word,1))
        .reduceByKey(lambda a,b: a+b)
    )
    max_word = rdd_word_count.max(key=lambda x: x[1])
    print(f"Most frequent word: {max_word[0]}, count: {max_word[1]}")

    for word, count in rdd_word_count.collect():
        print(f"{word}: {count}")

