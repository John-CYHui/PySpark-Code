from codecs import ascii_encode
from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    
    rdd = sc.parallelize(range(1,10), 3)
    
    rdd2 = rdd.glom().collect()
    print(rdd.fold(10, lambda a, b: a + b))
    