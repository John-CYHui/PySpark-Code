from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    
    rdd = sc.textFile("file:///home/dev/data/words.txt")
    
    words_rdd = rdd.flatMap(lambda x: x.split(" "))
    
    words_rdd_with_one = words_rdd.map(lambda x: (x,1))
    
    result_rdd = words_rdd_with_one.reduceByKey(lambda a, b: a + b)
    
    print(result_rdd.collect())