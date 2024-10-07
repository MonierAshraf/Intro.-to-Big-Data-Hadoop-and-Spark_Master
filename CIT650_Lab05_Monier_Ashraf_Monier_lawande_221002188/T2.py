from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("Spark Structured Streaming Lab").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Added includeTimestamp option 
lines = spark.readStream.format("socket").option("host","localhost").\
option("port",7788).option("includeTimestamp", "true").load()

# Split line into => word, Time 
words = lines.select(explode(split(lines.value, ' ')).alias('word'),lines.timestamp)

# Group by Timestamp, word
The_window_count = words.groupBy(window(words.timestamp, '10 seconds', '5 seconds'),words.word).count()

# Stream the output
query = The_window_count.writeStream.outputMode("complete").format("console").start()
query.awaitTermination()