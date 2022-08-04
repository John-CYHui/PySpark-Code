from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    
    rdd = sc.parallelize(list(range(6)))
    
    result = rdd.filter(lambda x: x % 2 == 0)
    
    print(result.collect())