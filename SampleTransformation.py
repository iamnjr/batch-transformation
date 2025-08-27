from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MyApp").getOrCreate()
df = spark.createDataFrame([(1, "foo", 890), (2, "bar", 740)], ["id", "text", "rank"])
df.withColumnRenamed("rank", "Pin").show()
spark.stop()