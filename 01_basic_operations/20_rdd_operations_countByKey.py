from codecs import ascii_encode
from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    
    rdd = sc.textFile("file:///home/dev/data/words.txt")
    rdd2 = rdd.flatMap(lambda x:x.split(" ")).map(lambda x: (x,1))
    
    result = rdd2.countByKey()
    
    print(result)