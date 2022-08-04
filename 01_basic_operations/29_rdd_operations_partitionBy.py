from codecs import ascii_encode
from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    
    rdd = sc.parallelize([('hadoop', 1), ('spark', 1), ('hello', 1),('flink', 1),('hadoop', 1),('spark', 1)])
    
    def process(key):
        if "hadoop" == key or "hello" == key: return 0
        if "spark" == key: return 1
        return 2
    
    # Use partitionBy
    print(rdd.partitionBy(3, process).glom().collect())
    
