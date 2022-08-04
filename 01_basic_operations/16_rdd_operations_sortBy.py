from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    
    rdd = sc.parallelize([('c', 3),('a', 4), ('b', 2), ('a', 6),('e', 5), ('n', 9),('a', 4)])
    
    rdd2 = rdd.sortBy(lambda x: x[1], ascending=True, numPartitions=1)
    
    print(rdd2.collect())
    
    print(rdd.sortBy(lambda x:x[0], ascending=False, numPartitions=1).collect())