from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Transformation").getOrCreate()

cartDf = spark.read.option("multiLine", True).json(r"C:\Users\Lokesh\PycharmProjects\batch-transformation\Input\carts_20250828_005259.json")
productDf = spark.read.option("multiLine", True).json(r"C:\Users\Lokesh\PycharmProjects\batch-transformation\Input\products_20250828_005257.json")
userDf = spark.read.option("multiLine", True).json(r"C:\Users\Lokesh\PycharmProjects\batch-transformation\Input\users_20250828_005258.json")

# cartDf.show(2, False)
productDf.show(2, False)
# userDf.show(2, False)
# cartDf.printSchema()
productDf.printSchema()
# userDf.printSchema()

