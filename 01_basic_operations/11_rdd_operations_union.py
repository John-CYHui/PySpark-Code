from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    
    rdd = sc.parallelize([1,2,3,4])
    rdd2 = sc.parallelize(["a", "b", "c"])
    rdd3 = rdd.union(rdd2)
    
    print(rdd3.collect())