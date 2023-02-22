from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .getOrCreate()
    
spark.sparkContext.setLogLevel("ERROR")

lines_df = spark.readStream.format("rate").option("rowsPerSecond", 1).load()

lines_df = lines_df.withColumn("score", col("value") % 5)
lines_df = lines_df.groupBy("score").count()

query = lines_df \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()