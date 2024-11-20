from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("uv").getOrCreate()
sc=spark.sparkContext
rdd1=sc.textFile("/home/local/Downloads/log")
rdd2=rdd1.map(lambda x:x.split()).map(lambda x:x[0])
rdd3=rdd2.distinct().map(lambda x:("uv",1))
rdd4=rdd3.reduceByKey(lambda x,y:x+y)
rdd4.saveAsTextFile("/home/local/Downloads/22")
sc.stop()
