from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_date
from pyspark.sql.functions import round
from pyspark.sql.functions import to_date, sum as _sum

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

cart_products = carts_flat.join(products_df, carts_flat.product_id == products_df.id).join(user_flat, "user_id")

cart_products = cart_products.withColumn(
    "total_price", round(col("price") * col("quantity"), 2)
)


daily_sales = cart_products.groupBy(to_date("date").alias("order_date")) \
                           .agg(_sum("total_price").alias("daily_revenue"))

daily_sales.show(20,False)
daily_sales.printSchema()

top_products = cart_products.groupBy("title") \
                            .agg(_sum("quantity").alias("total_sold")) \
                            .orderBy(col("total_sold").desc())
top_products.show(20,False)
top_products.printSchema()

output_path = "/Users/niranjankashyap/PycharmProjects/Batch-Transformation/data/silver"

daily_sales.write.mode("overwrite").parquet(f"{output_path}/daily_sales")
top_products.write.mode("overwrite").parquet(f"{output_path}/top_products")
cart_products.write.mode("overwrite").parquet(f"{output_path}/cart_details")
