from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("Spark Lab")
sc = SparkContext(conf=conf)
File = sc.textFile("Big_Data_Labs/lab3/Data/AliceInWonderLandPart1.txt")
tokenized = File.flatMap(lambda line: line.split(" "))
stop_w=['in','on','or','and','a','the','of','so','from']

wordTuples = tokenized.map(lambda word: (word, 1))
wordCounts = wordTuples.reduceByKey(lambda v1,v2:v1 +v2)

x=wordCounts.filter(lambda tup : tup[0] not in stop_w)

list_words = wordCounts.collect()
print(list_words)