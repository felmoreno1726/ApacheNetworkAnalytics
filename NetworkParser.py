from pyspark import SparkContext, SparkConf
from pyspark.sql import Row, SQLContext  

# this class takes in a text file representing a network and parses it into a graphframe representation
conf = SparkConf().setMaster("local").setAppName("Load Facebook Network")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)
path = "./network-examples/socfb-American75.mtx"
textFile = sc.textFile(path)
# Stream text file
lines = sc.textFile(path)
parts = lines.map(lambda l: l.split(" "))
# map stream
#first column person
people1 = parts.map(lambda p: Row(id=p[0], name=p[0]))
vertices1 = sqlContext.createDataFrame(people1)
#second column person
people2 = parts.map(lambda p: Row(id=p[1], name=p[1]))
vertices2 = sqlContext.createDataFrame(people2)

print("vertices 1")
vertices1.show()
print("count", vertices1.count())
print("distinct vertices 1")
distincV1 = vertices1.distinct()
distincV1.show()
print("count", distincV1.count())




print("vertices 2")
vertices2.show()
print("count", vertices2.count())
print("distinct vertices 2")
distincV2 = vertices2.distinct()
distincV2.show()
print("count", distincV2.count())


vertices = distincV1.join(distincV2, ["id", "name"], "outer")
print("vertices")
vertices.show()
print("count", vertices.count())
# remove duplicates
v = vertices.distinct()
print("v")
v.show()
print("v count", v.count())

# map stream to columns
peopleForward = parts.map(lambda p: Row(id1=p[0], id2=p[1]))
peopleBackward = parts.map(lambda p: Row(id1=p[1], id2=p[0]))
# dataFrame object of directed friendship relations
forwardFriendship = sqlContext.createDataFrame(peopleForward)
backwardFriendship = sqlContext.createDataFrame(peopleBackward)
# undirected friendship dataFrame
friendships = forwardFriendship.join(backwardFriendship, ["id1", "id2"], "outer")

print("schema 1")
forwardFriendship.show()
print("schema 2")
backwardFriendship.show()
print("schema")
friendships.show()
print(friendships.count())
