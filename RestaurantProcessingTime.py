from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, desc, asc

spark = SparkSession.builder \
    .appName("Restaurant Processing Time") \
    .enableHiveSupport() \
    .config("spark.sql.parquet.writeLegacyFormat",True)\
    .getOrCreate()

# Charger les tables depuis Hive
restaurantDetail = spark.read.parquet("hdfs://namenode:9000/user/spark/restaurant_detail_hive")
orderDetail = spark.read.parquet("hdfs://namenode:9000/user/spark/order_detail_hive")

# Calculer le temps moyen de traitement pour chaque catégorie de plat
avgProcessingTimePerCategory = orderDetail.join(restaurantDetail, orderDetail["restaurant_id"] == restaurantDetail["id"]) \
    .groupBy("category") \
    .agg(avg("estimated_cooking_time").alias("avg_processing_time")) \
    .orderBy(desc("avg_processing_time"))

# Afficher le temps moyen de traitement pour chaque catégorie de plat
print("Temps moyen de traitement pour chaque catégorie de plat :")
avgProcessingTimePerCategory.show()

# Calculer le top 3 des restaurants ayant un temps de préparation réduit
top3Restaurants = restaurantDetail.join(orderDetail, orderDetail["restaurant_id"] == restaurantDetail["id"]) \
    .groupBy("restaurant_name") \
    .agg(avg("estimated_cooking_time").alias("avg_cooking_time")) \
    .orderBy(asc("avg_cooking_time")) \
    .limit(3)

# Afficher le top 3 des restaurants ayant un temps de préparation réduit
print("Top 3 des restaurants avec un temps de préparation réduit :")
top3Restaurants.show()

# Enregistrer le temps moyen de traitement par catégorie dans un fichier CSV
avgProcessingTimePerCategory.write.csv("/avg_processing_time_per_category.csv")

# Enregistrer le top 3 des restaurants dans un fichier CSV
top3Restaurants.write.csv("/top_3_restaurants.csv")

spark.stop()