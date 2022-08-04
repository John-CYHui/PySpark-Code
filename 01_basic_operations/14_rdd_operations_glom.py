from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    
    rdd = sc.parallelize(list(range(1, 10)), 2)
    
    print(rdd.glom().flatMap(lambda x: x).collect())