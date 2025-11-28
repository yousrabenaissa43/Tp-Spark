from pyspark.sql import SparkSession

# Initialise SparkSession
spark = SparkSession.builder.appName("wordcount_py").getOrCreate()

# Crée un DataFrame avec les données
data = [
    ("Electronics", 1000),
    ("Clothing", 800),
    ("Electronics", 1200),
    ("Books", 300),
    ("Clothing", 600),
    ("Electronics", 900),
    ("Books", 500),
    ("Clothing", 700),
    ("Books", 400)
]

columns = ["category", "amount"]
df = spark.createDataFrame(data, columns)

# Apply batch transformations or analytics
result = df.groupBy("category").count()

# Collect top N records as per the requirement using DataFrame operations
top_records_df = result.orderBy(result['count'].desc()).limit(10)

# Show the top records DataFrame
top_records_df.show()

# Write the batch processed data to an output directory
top_records_df.write.csv("output")  # Saving as a directory 'output'
