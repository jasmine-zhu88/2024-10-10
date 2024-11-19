from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
conf=SparkConf().setAppName("testspark").setMaster("local")
sc=SparkContext(conf=conf)
print(sc.parallelize([1,2,3,4,5]).map(lambda x:x+1).reduce(lambda x,y:x+y))
sc.stop()
