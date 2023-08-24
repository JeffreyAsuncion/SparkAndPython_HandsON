# Import What We Need
from pyspark import SparkConf, SparkContext
import collections

# SEt Up Our Context, local, (single thread, single process)
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

# Load the Data
lines = sc.textFile("file:///mnt/c/Users/jeffr/Udemy_Spark/ml-100k/u.data")
# Extract(map) the data we care about
ratings = lines.map(lambda x: x.split()[2])
# Perform an Action: Count by Value
result = ratings.countByValue()

# straight python, not spark
# sort and display the results
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))