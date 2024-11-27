
"""
from pyspark import SparkConf,SparkContext
if __name__=="__main__":
    conf=SparkConf()
    sc=SparkContext(conf=conf)
    sc.textFile("/home/local/Downloads/pg20417.txt").flatMap(lambda line:line.strip().split()).map(lambda x:(x,1)).reduceByKey(lambda a,b:a+b).saveAsTextFile("/home/local/Downloads/wc")
    sc.stop()
    """

#run cmd in spark: ./spark-submit /home/local/Documents/Python-script/Gitfolder/2024-10-10/countword-spark.py /home/local/Downloads/pg4300.txt /home/local/Downloads/spark/



from pyspark import SparkConf,SparkContext
import sys
if __name__=="__main__":
    if len(sys.argv)!=3:
        print("Usage:wordcount <input> <output>",file=sys.stderr)
        sys.exit(-1)
    conf=SparkConf()
    sc=SparkContext(conf=conf)

    def printResult():
        counts=sc.textFile(sys.argv[1]).flatMap(lambda line:line.strip().split()).map(lambda x:(x,1)).reduceByKey(lambda a,b:a+b)
        output=counts.collect()
        for (word,count) in output:
            print(f"{word}:{count}")
    def saveFile():
        sc.textFile(sys.argv[1]).flatMap(lambda line:line.strip().split()).map(lambda x:(x,1)).reduceByKey(lambda a,b:a+b).saveAsTextFile(sys.argv[2])
    saveFile()
    sc.stop()
