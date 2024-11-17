from pyspark import SparkConf,SparkContext
if __name__=="__main__":
    conf=SparkConf()
    sc=SparkContext(conf=conf)

    def printResult():
        counts=sc.textFile("/home/local/Downloads/pg20417.txt").flatMap(lambda line:line.strip().split()).map(lambda x:(x,1)).reduceByKey(lambda a,b:a+b)
        output=counts.collect()
        for (word,count) in output:
            print(f"{word}:{count}")
    def saveFile():
        sc.textFile("/home/local/Downloads/pg20417.txt").flatMap(lambda line:line.strip().split()).map(lambda x:(x,1)).reduceByKey(lambda a,b:a+b).saveAsTextFile("/home/local/Downloads/wc.txt")
        saveFile()
        sc.stop()
