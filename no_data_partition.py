from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark =  SparkSession.builder.appName("NoDataPartition").master("local[3]").getOrCreate()

    data_df = spark.read.csv("data/test.csv", header=True, inferSchema=True)

    print("No of Partition : " , data_df.rdd.getNumPartitions())
    data_df = data_df.repartition(3)
    print("No of re - Partition : ", data_df.rdd.getNumPartitions())
    partition_data  = data_df.rdd.glom().collect()

    for idx, partition in enumerate(partition_data):
        print(f"partition ID : {idx}, Data : {partition}" )
    #data_df.show()
    data_df = data_df.coalesce(2)
    #data_df.rdd.mapPartitionsWithIndex(lambda idx1, itr: [(idx1, list(itr))])
    data_df.rdd.foreachPartition(lambda partitionnew: print(list(partitionnew)))
    input("Press Enter to continue...")