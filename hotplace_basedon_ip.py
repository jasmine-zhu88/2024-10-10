from pyspark.sql import SparkSession
import pyspark.pandas as ps
from pyspark.sql.types import *

def ip_transform(ip):
    ips=ip.split(".")
    ip_num=0
    for i in ips:
        ip_num=int(i)|ip_num <<8
    return ip_num
def binary_search(ip_num,broadcast_value):
    start=0
    end=len(broadcast_value)-1
    while(start<=end):
        mid=(start+end)//2
        if ip_num>=broadcast_value[mid][0] and ip_num<=broadcast_value[mid][1]:
            return mid
        if ip_num<broadcast_value[mid][0]:
            end=mid
        if ip_num>broadcast_value[mid][1]:
            start=mid
    


def main():
    spark=SparkSession.builder.appName("hotplace_basedon_ip").getOrCreate()
    sc=spark.sparkContext
#-------------------------------------------------------------
    #read the city ip and latitude and longtitue data in rdd
    city_ip_rdd=sc.textFile("/home/local/Downloads/ip_latitude_longitude").map(lambda x:x.split(",")).map(lambda x:[ip_transform(x[0])]+[ip_transform(x[1])]+x[2:])
    city_ip__rdd_broadcast=sc.broadcast(city_ip_rdd.collect()) 
    
#--------------------------------------------------------------    
    """read the city ip and latitude and longtitue data in spark dataframe"""
    #schema=StructType([StructField("ip_start",StringType()),StructField("ip_stop",StringType()),StructField("lat",StringType()),StructField("long",StringType())])
    #city_ip_df=spark.read.csv("/home/local/Downloads/ip_latitude_longitude",schema=schema)
    """or create spark dataframe based on rdd"""
    #city_ip_df=spark.createDataFrame(city_ip_rdd.collect(),["ip_start","ip_stop","lat","long"])
    #city_ip_df.withColumn("ip_start",ip_transform("ip_start")).show()
#--------------------------------------------------------------

    """convert the spark dataframe to pandas dataframe"""
    #pd_df=city_ip_df.pandas_api()
    #pd_df["ip_start"]=pd_df["ip_start"].map(ip_transform)
    #pd_df["ip_stop"]=pd_df["ip_stop"].map(ip_transform)

#--------------------------------------------------------------
    """read the data in spark rdd"""
    target_ip_rdd=sc.textFile("/home/local/Downloads/weblog_new.csv").map(lambda x:x.split(",")[0]).filter(lambda x:len(x)>4)
    #print(target_ip_rdd.take(5))
#--------------------------------------------------------------

    """read the data in spark dataframe"""
    #target_data_df=spark.read.csv("/home/local/Downloads/weblog_new.csv",header=True)
    #target_ip_df=target_data_df.select("IP")

    """convert the spark dataframe to pandas dataframe"""
    #target_ip_pd=target_data_df.pandas_api()
#--------------------------------------------------------------

    def get_pos(x):
        city_broadcast_value=city_ip__rdd_broadcast.value
        def get_result(ip):
            ip_num=ip_transform(ip)
            index=binary_search(ip_num,city_broadcast_value)
            return ((city_broadcast_value[index][2],city_broadcast_value[index][3]),1)
        x=map(tuple,[get_result(ip) for ip in x])
        return x
    

    result_rdd=target_ip_rdd.mapPartitions(lambda x:get_pos(x))
    result_rdd_=result_rdd.reduceByKey(lambda x,y:x+y)
    print(result_rdd_.collect())
    sc.stop()

if __name__=="__main__":
    main()