from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

# Initialize Spark
spark = SparkSession.builder \
    .appName("FakeStoreTransformation") \
    .getOrCreate()

# Base data path (adjust to your setup)
base_path = "/Users/niranjankashyap/PycharmProjects/Batch-Transformation/data/raw"

# Read JSON files
products_df = spark.read.option("multiline",True).json(f"{base_path}/products/*.json")
users_df = spark.read.option("multiline",True).json(f"{base_path}/users/*.json")
carts_df = spark.read.option("multiline",True).json(f"{base_path}/carts/*.json")

products_df.printSchema()
products_df.show(2,False)
users_df.printSchema()
users_df.show(2,False)
carts_df.printSchema()
carts_df.show(2,False)

user_flat = users_df.select(
    col("id").alias("user_id"),
    col("email"),
    col("username"),
    col("name.firstname").alias("first_name"),
    col("name.lastname").alias("last_name"),
    col("address.city").alias("city"),
    col("address.street").alias("street"),
    col("address.number").alias("street_number"),
    col("address.zipcode").alias("zipcode"),
    col("phone")
)

carts_flat = carts_df.select(
    col("id").alias("cart_id"),
    col("userId").alias("user_id"),
    col("date"),
    explode("products").alias("product")
).select(
    col("cart_id"),
    col("user_id"),
    col("date"),
    col("product.productId").alias("product_id"),
    col("product.quantity").alias("quantity")
)