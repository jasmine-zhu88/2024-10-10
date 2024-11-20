#from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("pv").getOrCreate()
sc=spark.sparkContext
rdd1=sc.textFile("/home/local/Downloads/log")
rdd2=rdd1.map(lambda x:("pv",1)).reduceByKey(lambda a,b:a+b)
print(rdd2.collect())
sc.stop()