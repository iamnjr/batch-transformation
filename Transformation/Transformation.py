
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_date, round, sum

spark = SparkSession.builder \
    .appName("Transformation") \
    .master("local[*]") \
    .getOrCreate()

cartDf = spark.read.option("multiLine", True).json(r"C:\Users\Lokesh\PycharmProjects\batch-transformation\Input\carts_20250828_005259.json")
productDf = spark.read.option("multiLine", True).json(r"C:\Users\Lokesh\PycharmProjects\batch-transformation\Input\products_20250828_005257.json")
userDf = spark.read.option("multiLine", True).json(r"C:\Users\Lokesh\PycharmProjects\batch-transformation\Input\users_20250828_005258.json")



users_flat = userDf.select(
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

carts_flat = cartDf.select(
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

cart_products = carts_flat.join(productDf, carts_flat.product_id == productDf.id).join(users_flat, "user_id")

cart_products = cart_products.withColumn(
    "total_price", round(col("price") * col("quantity"), 2)
)

daily_sales = cart_products.groupBy(to_date("date").alias("order_date")).agg(sum("total_price").alias("daily_revenue"))

daily_sales.show(20, False)
daily_sales.printSchema()

top_products = cart_products.groupBy("title") \
    .agg(sum("quantity").alias("total_sold")) \
    .orderBy(col("total_sold").desc())
top_products.show(20, False)
top_products.printSchema()

output_path = "C:/Users/Lokesh/PycharmProjects/batch-transformation/data/silver"

daily_sales.write.mode("overwrite").parquet(f"{output_path}/daily_sales")
top_products.write.mode("overwrite").parquet(f"{output_path}/top_products")
cart_products.write.mode("overwrite").parquet(f"{output_path}/cart_details")






