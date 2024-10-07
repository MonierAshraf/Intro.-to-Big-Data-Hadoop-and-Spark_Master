from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("Spark Lab")
sc = SparkContext(conf=conf)
File = sc.textFile("Big_Data_Labs/lab3/u.data")

tuple_pairs = File.map(lambda line: (int(line.split("\t")[1]),int(line.split("\t")[2])))
rate_count = tuple_pairs.reduceByKey(lambda v1,v2:v1+v2)
#rateCounts.take(15)

mov_Tup = rate_count.map(lambda word: (word, 1))
#mov_Tup.take(15)
mov_count = mov_Tup.reduceByKey(lambda v1 , v2 : v1 + v2 )
#mov_count.take(15)


oneList = rate_count+mov_count

#print(oneList.take(15))
final = oneList.reduceByKey(lambda v1,v2:(v1/v2))
print(final.take(200))