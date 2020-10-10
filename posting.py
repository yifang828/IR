from pyspark import SparkConf, SparkContext
from bs4 import BeautifulSoup
import re, string

conf = SparkConf().setAppName("posting")
conf = conf.setMaster("spark://10.0.2.15:7077")
sc = SparkContext(conf = conf)
sc._jsc.hadoopConfiguration().set("textinputformat.record.delimiter", "WARC/0.18\n")

def combine(previous, current):
    if type(previous) is not list and type(current) is not list:
        return [previous, current]
    if type(previous) is list and type(current) is not list:
        previous.append(current)
        return previous
    if type(previous) is not list and type(current) is list:
        current.append(previous)
        return current
    if type(previous) is list and type(current) is list:
        return previous + current

def parse_body(document):
    beautifulSoup = BeautifulSoup(document, "html.parser")
    try:
        return uni_to_clean_str(beautifulSoup.body.text).split(" ")
    except:
        return []

def uni_to_clean_str(x):
    x = x.lower()
    translator = str.maketrans('', '', string.punctuation)
    clean_str = x.translate(translator)
    clean_str = re.sub('\W', ' ', clean_str)
    clean_str = re.sub('\d', ' ', clean_str)
    return clean_str

data = sc.textFile("10.warc")
data = data.map(lambda document: parse_body(document))\
        .map(lambda termInDoc: [term for term in termInDoc if term != ""])

# [(term, position)]
data = data.map(lambda termInDoc: [(termInDoc[position], position) for position in range(len(termInDoc))])

# ([(term, position),...], article) --> [(term, article), position]
data = data.zipWithIndex()
data = data.flatMap(lambda termInDoc: [((term, termInDoc[1]), position) for term, position in termInDoc[0]])

# [(term, article), [positions]] --> [(term, article), position] + [(term, article), position]
# data = data.reduceByKey(lambda previous, current: (previous.append(current)) if type(previous) is list else [previous, current])
data = data.reduceByKey(lambda previous, current: combine(previous, current))

# (term, (article, [position]))
# (term, (article, repeat count, [position]))
data = data.map(lambda termInDoc:(termInDoc[0][0], (termInDoc[0][1], [termInDoc[1]] if type(termInDoc[1]) is not list else termInDoc[1])))
data = data.map(lambda termInDoc:(termInDoc[0], (termInDoc[1][0], len(termInDoc[1][1]), termInDoc[1][1])))

# (term, [(article, repeat count, [position])])
data = data.reduceByKey(lambda previous, current: combine(previous, current))

# (term, documentFrequency, [(article, repeat count, [position])])
postingRdd = data.map(lambda termInDoc: (termInDoc[0], len(termInDoc[1]), termInDoc[1]))

# 存檔
data.coalesce(1).saveAsTextFile("pos")
