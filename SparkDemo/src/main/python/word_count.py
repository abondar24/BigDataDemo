from pyspark import SparkContext

import re, sys

sc = SparkContext("local", "WordCount")
input = sc.textFile(sys.argv[1])
words = input.flatMap(lambda x: x.split(" "))
res = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
res.saveAsTextFile(sys.argv[2])