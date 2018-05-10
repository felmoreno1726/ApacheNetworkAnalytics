from pyspark import SparkContext, SparkConf
from pyspark.sql import Row, SQLContext  
from Metric import *
import time

def get_v(parts):
    #first column person
    people1 = parts.map(lambda p: Row(id=p[0], name=p[0]))
    #second column person
    people2 = parts.map(lambda p: Row(id=p[1], name=p[1]))
    #single column dataframes
    vertices1 = sqlContext.createDataFrame(people1)
    vertices2 = sqlContext.createDataFrame(people2)
    #join into one dataframe
    vertices = vertices1.join(vertices2, ["id", "name"], "outer")
    #remove repeated
    return vertices.distinct()

def get_e(parts):
    # map stream to columns
    peopleForward = parts.map(lambda p: Row(src=p[0], dst=p[1]))
    peopleBackward = parts.map(lambda p: Row(src=p[1], dst=p[0]))
    # dataFrame object of directed friendship relations
    forwardFriendship = sqlContext.createDataFrame(peopleForward)
    backwardFriendship = sqlContext.createDataFrame(peopleBackward)
    # undirected friendship dataFrame
    friendships = forwardFriendship.join(backwardFriendship, ["src", "dst"], "outer")
    #friendships.show()
    return friendships.distinct()

# this class takes in a text file representing a network and parses it into a graphframe representation
conf = SparkConf().setMaster("local").setAppName("Load Facebook Network")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)
path = "./network-examples/example.txt"#REPLACE path with American, MIT, and Harvard friendship networks
textFile = sc.textFile(path)
# Stream text file
lines = sc.textFile(path)
parts = lines.map(lambda l: l.split(" "))
# vertices dataframe
time_before_v = time.clock()
V = get_v(parts)
time_after_v = time.clock()
print("vertices dataframe build time: ", time_after_v - time_before_v)
# edges dataframe
time_before_e = time.clock()
E = get_e(parts)
time_after_e = time.clock()
print("edges dataframe build time: ", time_after_e - time_before_e)
#metric
time_before_metric_obj = time.clock()
met_obj = Metric(V, E, sqlContext)
time_after_metric_obj = time.clock()
print("metric object build time: ", time_after_metric_obj - time_before_metric_obj)

#clustering
time_before_clustering_coefficient = time.clock()
print("clustering coefficient: ", met_obj.overall_clustering_coefficient())
time_after_clustering_coefficient = time.clock()
print("clustering coefficient computation time: ", time_after_clustering_coefficient - time_before_clustering_coefficient)

#diameter
time_before_diameter = time.clock()
print(met_obj.diameter())
time_after_diameter = time.clock()
print("diameter computation time: ", time_after_diameter - time_before_diameter)

#average_path_length
time_before_path = time.clock()
print("average path length: ", met_obj.average_path_length())
time_after_path = time.clock()
print("avg path length computation time: ", time_after_path - time_before_path)

#closenes_centrality
time_before_centrality = time.clock()
print("closeness centrality: ", met_obj.closeness_centrality())
time_after_centrality = time.clock()
print("closeness centrality computation time: ", time_after_centrality - time_before_centrality)
