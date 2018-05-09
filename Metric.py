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
        row_triangles = num_triangles_frame.agg({"count":"sum"}).collect()[0]
        num_triangles = row_triangles.asDict()['sum(count)']

        # calculate the number of triples
        degrees_frame = degrees_frame.withColumn("triples", degrees_frame.inDegree*(degrees_frame.inDegree-1)/2.0)
        row_triples = degrees_frame.agg({"triples":"sum"}).collect()[0]
        num_triples = row_triples.asDict()['sum(triples)']

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

