# coding:utf8
from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("WordCountHelloWorld")
    # Build sparkContext object
    sc = SparkContext(conf=conf)
    
    file_rdd = sc.textFile("hdfs://master:9000/inputs/words.txt")
    #file_rdd = sc.textFile("file:///home/dev/data/words.txt")
    
    
    # split words
    words_rdd = file_rdd.flatMap(lambda line: line.split(" "))
    words_with_one_rdd = words_rdd.map(lambda x: (x, 1))
    
    result_rdd = words_with_one_rdd.reduceByKey(lambda a, b: a+b)
    
    print(result_rdd.collect())
