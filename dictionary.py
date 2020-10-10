from pyspark import SparkConf, SparkContext
from bs4 import BeautifulSoup
import re

conf = SparkConf().setAppName("dictionary")
conf = conf.setMaster("spark://10.0.2.15:7077")
sc = SparkContext(conf = conf)
sc._jsc.hadoopConfiguration().set("textinputformat.record.delimiter", "WARC/0.18\n")

def parse_body(document):
    beautifulSoup = BeautifulSoup(document, "html.parser")
    try:
        return uni_to_clean_str(beautifulSoup.body.text).split(" ")
    except:
        return []

def uni_to_clean_str(body):
    body = body.lower()
    body = body.replace('_', ' ')
    body = re.sub('\s', ' ', body)
    body = re.sub('\W', ' ', body)
    body = re.sub('\d', ' ', body)
    return body

rdd = sc.textFile("10.warc")
rdd = rdd.filter(lambda document: document !="")

# rdd = rdd.mape(lambda document: parse_body(document))
# print(rdd)
rdd = rdd.map(lambda document: parse_body(document))\
    .map(lambda termInDoc: [term for term in termInDoc if term != ""])\
    .map(lambda termInDoc: [(termInDoc[index], index) for index in range(len(termInDoc))])\
    .zipWithIndex()

# rdd.coalesce(1).saveAsTextFile("HW1_Dictionary")

# dictionary
dictionaryRdd = rdd.flatMap(lambda termInDoc: ([term[0], termInDoc[1]] for term in termInDoc[0]))\
            .groupByKey()\
            .sortByKey()\
            .map(lambda x: (x[0], len(list(dict.fromkeys(list(x[1]))))))
dictionaryRdd.coalesce(1).saveAsTextFile("dictionary")
