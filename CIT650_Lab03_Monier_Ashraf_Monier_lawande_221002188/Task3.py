
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("Spark Lab")
sc = SparkContext(conf=conf)
File = sc.textFile("Big_Data_Labs/lab3/Data/AliceInWonderLandPart1.txt")
tokenized = File.flatMap(lambda line: line.split(" "))
wordTuples = tokenized.map(lambda word: (word, 1))
import string
alphabet = list(string.ascii_lowercase)
task3=alphabet[1:]
t3=tuple(task3)
y=wordTuples.collect()
y=wordTuples.map(lambda x: x[0].lower())
y1=y.filter(lambda word:word.startswith((t3)))
z=y1.collect()

wordCounts = wordTuples.reduceByKey(lambda v1,v2:v1 +v2)


list_words = wordCounts.collect()

print(z)