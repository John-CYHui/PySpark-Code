from codecs import ascii_encode
from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    
    rdd = sc.parallelize([1,3,5,6,3,2,6,7,8],1)
    
    print(rdd.takeOrdered(3))
    
    print(rdd.takeOrdered(3, lambda x: -x))
    