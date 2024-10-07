from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("Spark Lab")
sc = SparkContext(conf=conf)
File = sc.textFile("Big_Data_Labs/lab3/Data/AliceInWonderLandPart1.txt")
tokenized = File.flatMap(lambda line: line.split(" "))
wordTuples = tokenized.map(lambda word: (word, len(word)))

type(wordTuples)
x=tuple(wordTuples.collect())

max_tuple = max(x, key=lambda tup: tup[1])

print(max_tuple)