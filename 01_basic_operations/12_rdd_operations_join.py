from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    
    rdd1 = sc.parallelize([(1001, "john"), (1002, "sam"), (1003, "alfred"), (1004, "zack")])
    rdd2 = sc.parallelize([(1001, "sales department"), (1002, "tech department")])
    
    print(rdd1.join(rdd2).collect())
    
    print(rdd1.leftOuterJoin(rdd2).collect())