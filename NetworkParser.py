from pyspark import SparkContext, SparkConf
from pyspark.sql import Row, SQLContext  
import Metric

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
    peopleForward = parts.map(lambda p: Row(id1=p[0], id2=p[1]))
    peopleBackward = parts.map(lambda p: Row(id1=p[1], id2=p[0]))
    # dataFrame object of directed friendship relations
    forwardFriendship = sqlContext.createDataFrame(peopleForward)
    backwardFriendship = sqlContext.createDataFrame(peopleBackward)
    # undirected friendship dataFrame
    friendships = forwardFriendship.join(backwardFriendship, ["id1", "id2"], "outer")
    #friendships.show()
    return friendships

# this class takes in a text file representing a network and parses it into a graphframe representation
conf = SparkConf().setMaster("local").setAppName("Load Facebook Network")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)
path = "./network-examples/socfb-American75.mtx"
textFile = sc.textFile(path)
# Stream text file
lines = sc.textFile(path)
parts = lines.map(lambda l: l.split(" "))
# vertices dataframe
V = get_v(parts)
# edges dataframe
E = get_e(parts)
#run metrics on the graph
met_obj = Metric(V, E)