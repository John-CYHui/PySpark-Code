from codecs import ascii_encode
from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    
    rdd = sc.parallelize(list(range(1, 11)), 2)
    
    count = sc.accumulator(0)
    
    def map_func(data):
        global count
        count += 1
        # print(count)
    
    rdd2 = rdd.map(map_func)
    rdd2.cache()
    rdd2.collect()
    
    rdd3 = rdd2.map(lambda x:x)
    rdd3.collect()
    print(count)