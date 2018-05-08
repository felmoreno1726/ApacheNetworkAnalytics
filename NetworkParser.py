from pyspark import SparkContext, SparkConf
import os

# this class takes in a text file representing a network and parses it into a graphframe representation
conf = SparkConf().setMaster("local").setAppName("Load Facebook Network")
sc = SparkContext(conf = conf)
path = "/network-examples/example.txt"
textFile = sc.textFile(path)
textFile.collect()


#data = [1, 2, 3, 4, 5]
#distData = sc.parallelize(data)