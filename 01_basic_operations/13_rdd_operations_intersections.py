from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    
    rdd1 = sc.parallelize([("a", 1), ("a", 3)])
    rdd2 = sc.parallelize([("a", 1), ("b", 3)])
    
    rdd3 = rdd1.intersection(rdd2)
    print(rdd3.collect())