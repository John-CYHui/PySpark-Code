from codecs import ascii_encode
from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    
    rdd = sc.parallelize([('c', 3),('A', 4), ('b', 2), ('a', 6),('e', 5), ('n', 9),('a', 4),
                          ('y', 1),('u', 2), ('i', 1), ('o', 1),('M', 2), ('k', 6),('b', 2),])
    
    print(rdd.sortByKey(ascending=True, numPartitions=1, keyfunc=lambda key: str(key).lower()).collect())