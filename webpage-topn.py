from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("topN").getOrCreate()
sc=spark.sparkContext
rdd1=sc.textFile("/home/local/Downloads/log")
rdd2=rdd1.map(lambda x:x.split()).filter(lambda x:len(x)>10).map(lambda x:(x[10],1))
rdd3=rdd2.reduceByKey(lambda a,b:a+b).sortBy(lambda x:x[1],ascending=False)
rdd4=rdd3.take(2)
print(rdd4)
sc.stop()