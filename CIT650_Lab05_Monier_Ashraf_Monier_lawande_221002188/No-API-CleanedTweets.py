import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
import pyspark.sql.functions as psf
from datetime import datetime
import pytz
from pyspark.sql.functions import udf, to_date, to_utc_timestamp
import re
from collections import Counter
from nltk.corpus import stopwords
from gensim.parsing.preprocessing import remove_stopwords

spark = SparkSession\
                .builder\
                .appName("tweets")\
                .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Tweets.csv Schema
schema = StructType([StructField("tweet_id", StringType(), True),\
                    StructField("in_reply_to_status_id", StringType(), True),\
                    StructField("in_reply_to_user_id", StringType(), True),\
                    StructField("retweeted_status_id", StringType(), True),\
                    StructField("retweeted_status_user_id", StringType(), True),\
                    StructField("timestamp", StringType(), True),\
                    StructField("source", StringType(), True),\
                    StructField("text", StringType(), True),\
                    StructField("expanded_urls", StringType(), True)
                                ]
                               )

df_tweets = spark.readStream.option("sep", ",").option("header", "true").schema(schema).csv("/home/bitnami/Big_Data_Labs/lab6/tweets")


tweets = df_tweets.select("text")



def CleanTweet(tweet):
    cleaned = re.sub("[^A-Za-z\s]", "", str(tweet)) 
    cleaned1=remove_stopwords(cleaned)
    truncated = " ".join(cleaned1.split()[:5])

    
    
    
    return truncated

Clean_Tweet_udf = udf(CleanTweet, StringType())

resultDF = tweets.withColumn("text", Clean_Tweet_udf(tweets.text))


query = resultDF.writeStream.outputMode("append").format("console").option("truncate", "false").start().awaitTermination()
