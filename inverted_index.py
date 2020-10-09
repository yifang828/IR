from pyspark.sql import SparkSession
from pyspark import SparkContext

spark = SparkSession.builder.master("local").getOrCreate()
sc = SparkContext.getOrCreate()

#simple spark 
# rdd = sc.parallelize(["hello world"])
# count = rdd.flatMap(lambda x: x.split(' ')).map(lambda word: (word, 1)).reduceByKey((lambda a, b: a+b))
# output = count.collect()
# print(output)

#wordcount 
# data = sc.textFile("./10.warc.gz")
# count = data.flatMap(lambda x: x.split(' ')).map(lambda word: (word, 1)).reduceByKey((lambda a, b: a+b))
# output = count.collect()
# output

#read warc and write to txt
import warc

f = warc.open("10.warc.gz")
inf = open("output.txt", "a")
for record in f:
    inf.write(str(record))
inf.close()
f.close()

# 