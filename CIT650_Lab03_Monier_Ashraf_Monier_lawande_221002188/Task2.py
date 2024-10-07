import sys
countThreshold = sys.argv[1]
#print(countThreshold)

from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("Spark Lab")
sc = SparkContext(conf=conf)
File = sc.textFile("Big_Data_Labs/lab3/Data/AliceInWonderLandPart1.txt")
tokenized = File.flatMap(lambda line: line.split(" "))
wordTuples = tokenized.map(lambda word: (word, 1))
y=wordTuples.filter(lambda word:word[1] < int(countThreshold))


wordCounts = wordTuples.reduceByKey(lambda v1,v2:v1 +v2)

z=y.collect()




list_words = wordCounts.collect()

print(z)