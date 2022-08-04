from codecs import ascii_encode
from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    
    rdd = sc.parallelize([list(range(10))], 3)
    
    #print(rdd.repartition(1).getNumPartitions())

    #print(rdd.repartition(5).getNumPartitions())
    
    print(rdd.coalesce(1).getNumPartitions())
    print(rdd.coalesce(5, shuffle=True).getNumPartitions())
