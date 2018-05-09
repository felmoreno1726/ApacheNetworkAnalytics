import numpy as np
from pyspark.sql import SparkSession
import pyspark
from graphframes import *
from pyspark import SparkContext, SparkConf, sql

class Metric(object):
    def __init__(self, vertices, edges):
        self.graphframes = Graph

    def overall_clustering_coefficient(self):
        num_triangles = self.graphframes.triangleCount()

    def diameter(self):
        pass

    def average_path_length(self):
        pass

    def closeness_centrality(self):
        pass

conf = SparkConf().setMaster("local").setAppName("Load Facebook Network")
sc = SparkContext(conf = conf)
sqlContext = sql.SQLContext(sc)

#v = sql.SQLContext.createDataFrame(sqlContext, [
#    ("1", "1"),
#    ("2", "2"),
#    ("3", "3"),
#    ("4", "4"),
#    ("5", "5")], ["id", "name"])

vertices = sql.SQLContext.createDataFrame(sqlContext, [
  ("a", "Alice", 34),
  ("b", "Bob", 36),
  ("c", "Charlie", 30),
  ("d", "David", 29),
  ("e", "Esther", 32),
  ("f", "Fanny", 36),
  ("g", "Gabby", 60)], ["id", "name", "age"])

edges = sql.SQLContext.createDataFrame(sqlContext, [
  ("a", "b", "friend"),
  ("b", "c", "follow"),
  ("c", "b", "follow"),
  ("f", "c", "follow"),
  ("e", "f", "follow"),
  ("e", "d", "friend"),
  ("d", "a", "friend"),
  ("a", "e", "friend")
], ["src", "dst", "relationship"])

#v = sql.SQLContext.createDataFrame(sqlContext, [
#    ("a", "Alice", "2""4"),
#    ("b", "Bob", "2""4"),
#    ("c", "Charlie", "2""4"),
#    ], ["id", "name", "age"])

#edges = [("1","2"),("2","1"),("1","4"),("4","1"),("2","3"),("3","2"),("2","4"),("4","2"),("2","5"),("5","2"),("3","5"),("5","3")]
#e = sql.SQLContext.createDataFrame(sqlContext, edges, ["src", "dst"])

#e = sql.SQLContext.createDataFrame(sqlContext, [
#  ("a", "b", "friend"),
#  ("b", "c", "follow"),
#  ("c", "b", "follow"),
#], ["src", "dst", "relationship"])

g = GraphFrame(vertices, edges)

results = g.triangleCount()
rows = results.collect()
for row in rows:
    print(row.asDict()["name"])
    print(row.asDict()["count"])

#metric = Metric(g)
#print(metric.overall_clustering_coefficient())

    #'''
    #    Creates a new metric object
    #    param: network
    #        -> numpy double-array representing adjacency matrix
    #        -> i.e. network[i][j] = 1 iff nodes i and j are connected
    #                            = 0 else
    #'''
    #def __init__(self, network):
    #    (m, n) = network.shape
    #    if m != n:
    #        raise Exception("adjacency matrix not square")

    #    self.network = network
    #    self.size = n

    #'''
    #    Calculates the overall clustering coefficient CL of this network
    #    CL = number of triangles in network / number of connected triples
    #'''
    #def overall_clustering_coefficient(self):
    #    num_triangles = 0
    #    num_conn_triples = 0
    #    for i in range(self.size):
    #        for j in range(i+1, self.size):
    #            for k in range(j+1, self.size):
    #                # increment if i,j,k form a triangle (all connected to each other)
    #                if self.forms_triangle(i, j, k):
    #                    num_triangles += 1


    #    for i in range(self.size):
    #        for j in range(self.size):
    #            for k in range(self.size):
    #                # increment if i,j,k are a connected triple
    #                if self.is_connected_triple(i, j, k):
    #                    num_conn_triples += 1
    #    # double-counted, so divide by 2
    #    num_conn_triples = num_conn_triples/2

    #    return 3.0*num_triangles/num_conn_triples

    #'''
    #    Check if nodes i, j, and k form a complete graph (all are connected to each other)
    #'''
    #def forms_triangle(self, i, j, k):
    #    i_j_connected = self.is_connected(i, j)
    #    i_k_connected = self.is_connected(i, k)
    #    j_k_connected = self.is_connected(j, k)
    #    return i_j_connected and i_k_connected and j_k_connected

    #'''
    #    Check if nodes i, j, and k form a connected triple
    #    - i.e. there is an edge between i and j AND an edge between j and k
    #    - where i, j, and k are DISTINCT nodes
    #'''
    #def is_connected_triple(self, i, j, k):
    #    # return False for non-distinct triples of nodes
    #    if i==j or j==k or k==i:
    #        return False

    #    i_j_connected = self.is_connected(i, j)
    #    j_k_connected = self.is_connected(j, k)

    #    return i_j_connected and j_k_connected

    #'''
    #    Check if node i and j are connected
    #'''
    #def is_connected(self, i, j):
    #    i_to_j = self.network[i][j] != 0
    #    j_to_i = self.network[j][i] != 0

    #    # if there is an edge from node i to node j OR node j to node i,
    #    #   then node i and node j are connected
    #    return i_to_j or j_to_i


