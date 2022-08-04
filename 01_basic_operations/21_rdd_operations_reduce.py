from codecs import ascii_encode
from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    
    rdd = sc.parallelize(range(1,6))
    
    result = rdd.reduce(lambda a, b: a + b)
    
    print(result)