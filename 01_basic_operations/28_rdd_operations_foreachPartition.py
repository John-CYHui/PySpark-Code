from codecs import ascii_encode
from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    
    rdd = sc.parallelize([1,3,5,6,3,2,6,7,8], 3)
    
    def process(iter):
        result = list()
        for it in iter:
            result.append(it * 10)
            
        print(result)
    
    rdd.foreachPartition(process)