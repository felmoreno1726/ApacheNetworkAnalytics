from pyspark import SparkContext, SparkConf
from pyspark.sql import Row, SQLContext
import os

# this class takes in a text file representing a network and parses it into a graphframe representation
conf = SparkConf().setMaster("local").setAppName("Load Facebook Network")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)
path = "./network-examples/socfb-American75.mtx"
textFile = sc.textFile(path)
#print(textFile.collect())
lines = sc.textFile(path)
parts = lines.map(lambda l: l.split(" "))
peopleForward = parts.map(lambda p: Row(id1=p[0], id2=p[1]))
peopleBackward = parts.map(lambda p: Row(id1=p[1], id2=p[0]))
schemaPeople1 = sqlContext.createDataFrame(peopleForward)
schemaPeople2 = sqlContext.createDataFrame(peopleBackward)
schemaPeople = schemaPeople1.join(schemaPeople2)
schemaPeople.show()
print(schemaPeople.count())
#data = [1, 2, 3, 4, 5]
#distData = sc.parallelize(data)