from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct

# Initialize Spark session
spark = SparkSession.builder.appName("CompanyUsersLang").getOrCreate()

# Create the data
data = [
    ("A", 1, "English"),("A", 1, "French"), ("A", 2, "English"),("A", 2, "French"),
    ("A", 3, "French"),("A", 3, "English"),("B", 1, "English"),("B", 2, "French"),
    ("C", 1, "English"),("C", 1, "French"),("B", 1, "French"),("B", 2, "English"),
]
# Define schema
columns = ["companyId", "userId", "language"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Step 1: Find users who speak both English and German in each company
user_langs = df.groupBy("companyId", "userId").agg(countDistinct("language").alias("lang_count"))

# Step 2: Filter users who speak both languages (count == 2)
bilingual_users = user_langs.filter(col("lang_count") == 2)

# Step 3: Count bilingual users per company
company_bilingual_counts = bilingual_users.groupBy("companyId").count()

# Step 4: Filter companies that have at least 2 such users
result = company_bilingual_counts.filter(col("count") >= 2).select("companyId")

# Show result
result.show()
