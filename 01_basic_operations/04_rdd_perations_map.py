# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    
    rdd = sc.parallelize([1,2,3,4,5,6])
    
    def multiply(data):
        return data * 10
    
    print(rdd.map(multiply).collect())