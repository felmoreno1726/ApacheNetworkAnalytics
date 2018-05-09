import numpy as np
from pyspark.sql import SparkSession
import pyspark
from graphframes import *
from pyspark import SparkContext, SparkConf, sql

class Metric(object):
    def __init__(self, vertices, edges):
        self.graphframes = GraphFrame(vertices, edges)
        self.vertices = [row.asDict()["id"] for row in vertices.collect()]
        self.shortest_paths = self.graphframes.shortestPaths(landmarks=self.vertices)

    def overall_clustering_coefficient(self):
        # import frames containing num_triangles
        num_triangles_frame = self.graphframes.triangleCount()
        # import frames containing degrees
        degrees_frame = self.graphframes.inDegrees

        # caculate the number of triangles, x3
        num_triangles = 0
        for row in num_triangles_frame.collect():
            num_triangles += row.asDict()["count"] 

        # calculate the number of triples
        num_triples = 0
        for row in degrees_frame.collect():
            d = row.asDict()["inDegree"]
            num_triples += d*(d-1)/2

        return 1.0*num_triangles/num_triples

    def diameter(self):
        max_d = 0
        for row in self.shortest_paths.collect():
            dictionary = row.asDict()["distances"]
            d = max(dictionary.values())
            max_d = max(d, max_d)
        return max_d

    def average_path_length(self):
        sum_d = 0
        for row in self.shortest_paths.collect():
            dictionary = row.asDict()["distances"]
            sum_d += sum(dictionary.values())

        n = len(self.vertices)

        return 1.0*sum_d/(n*(n-1))

    def closeness_centrality(self):
        closeness_dict = {}
        for row in self.shortest_paths.collect():
            dictionary = row.asDict()["distances"]
            v = row.asDict()["id"]
            sum_d = sum(dictionary.values())
            n = len(self.vertices)
            ci = 1.0*(n-1)/sum_d
            closeness_dict[v] = ci

        return closeness_dict

conf = SparkConf().setMaster("local").setAppName("Load Facebook Network")
sc = SparkContext(conf = conf)
sqlContext = sql.SQLContext(sc)

v = sql.SQLContext.createDataFrame(sqlContext, [
    ("1", "1"),
    ("2", "2"),
    ("3", "3"),
    ("4", "4"),
    ("5", "5")], ["id", "name"])

#vertices = sql.SQLContext.createDataFrame(sqlContext, [
#  ("a", "Alice", 34),
#  ("b", "Bob", 36),
#  ("c", "Charlie", 30),
#  ("d", "David", 29),
#  ("e", "Esther", 32),
#  ("f", "Fanny", 36),
#  ("g", "Gabby", 60)], ["id", "name", "age"])
#
#edges = sql.SQLContext.createDataFrame(sqlContext, [
#  ("a", "b", "friend"),
#  ("b", "c", "follow"),
#  ("c", "b", "follow"),
#  ("f", "c", "follow"),
#  ("e", "f", "follow"),
#  ("e", "d", "friend"),
#  ("d", "a", "friend"),
#  ("a", "e", "friend")
#], ["src", "dst", "relationship"])

#v = sql.SQLContext.createDataFrame(sqlContext, [
#    ("a", "Alice", "2""4"),
#    ("b", "Bob", "2""4"),
#    ("c", "Charlie", "2""4"),
#    ], ["id", "name", "age"])

edges = [("1","2"),("2","1"),("1","4"),("4","1"),("2","3"),("3","2"),("2","4"),("4","2"),("2","5"),("5","2"),("3","5"),("5","3")]
e = sql.SQLContext.createDataFrame(sqlContext, edges, ["src", "dst"])

metric = Metric(v, e)
#metric.overall_clustering_coefficient()
#metric.diameter()
#print(metric.average_path_length())
#print(metric.closeness_centrality())

#e = sql.SQLContext.createDataFrame(sqlContext, [
#  ("a", "b", "friend"),
#  ("b", "c", "follow"),
#  ("c", "b", "follow"),
#], ["src", "dst", "relationship"])

#g = GraphFrame(vertices, edges)
#
#results = g.triangleCount()
#rows = results.collect()
#for row in rows:
#    print(row.asDict()["name"])
#    print(row.asDict()["count"])