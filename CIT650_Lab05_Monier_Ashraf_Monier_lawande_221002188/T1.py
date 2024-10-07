from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split


sc = SparkContext()
spark = SparkSession(sc).builder.appName("StructuredNetworkWordCount").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
lines = spark.readStream.format("socket").option("host", "localhost").option("port", 7788).load()
words = lines.select(explode(split(lines.value, " ")).alias("word"))
wordCounts = words.groupBy("word").count()
query = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()