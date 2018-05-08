from pyspark import SparkContext

# this class takes in a text file representing a network and parses it into a graphframe representation
sc = SparkContext(master= 'local[2]')
textFile = sc.textFile('./socfb-American75.mtx')
