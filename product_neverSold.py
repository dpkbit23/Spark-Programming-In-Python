from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if __name__ == "__main__":
    spark = SparkSession.builder.appName("neverSold").master("local[*]").getOrCreate()

    productData = [(1, "Laptop"), (2, "Tablet"),
                     (3, "Smartphone"), (4, "Monitor"),
                     (5, "Keyboard") ]
    productColumns = ["product_id", "product_name"]
    productDF = spark.createDataFrame(productData, productColumns)
    productDF.show()

    SalesData = [(101, 1, "2025-01-01"), (102, 3, "2025-01-02"),
                  (103, 5, "2025-01-03") ]
    salesColumn = ["sale_id", "product_id", "sale_date"]
    salesDF = spark.createDataFrame(SalesData, salesColumn)
    salesDF.show()

    #join if column names are different in the join key
    neverSaleDF = productDF.join(salesDF, productDF["product_id"]== salesDF["product_id"], how="left")
    neverSaleDF.filter(col("sale_id").isNull()).drop(salesDF["product_id"]).drop("sale_id","sale_date").show()

    #join if column name are same in the join key
    neverSaleDF1 = productDF.join(salesDF, on="product_id", how="left_anti")
    productDF.join(salesDF, on="product_id", how="left_anti").explain(True)
    neverSaleDF1.show()