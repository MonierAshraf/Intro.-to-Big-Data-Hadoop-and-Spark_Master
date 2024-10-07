from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import  col


sparkSession = SparkSession.builder.master("local")\
.appName("ProcessingStreamFromFiles").getOrCreate()

                        
sparkSession.sparkContext.setLogLevel("ERROR")

schema = StructType([StructField("lsoa_code", StringType(), True),\
                     StructField("borough", StringType(), True),\
                     StructField("major_category", StringType(), True),\
                     StructField("minor_category", StringType(), True),\
                     StructField("value", StringType(), True),\
                     StructField("year", StringType(), True),\
                     StructField("month", StringType(), True)])


fileStreamDF = sparkSession.readStream\
               .option("header", "true")\
               .option("maxFilesPerTrigger", 1)\
               .schema(schema)\
               .csv("Big_Data_Labs/lab6/Data/subdata")


recordsPerBorough = fileStreamDF.groupBy("borough")\
                    .count()\
                    .orderBy("count", ascending=False)



query = recordsPerBorough.writeStream\
        .outputMode("complete")\
        .format("console")\
        .option("truncate", "false")\
         .option("numRows", 20)\
         .start()\
         .awaitTermination()
