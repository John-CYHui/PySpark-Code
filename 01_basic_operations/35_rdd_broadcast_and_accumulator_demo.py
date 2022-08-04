from codecs import ascii_encode
from pyspark import SparkConf, SparkContext
import re

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    
    file_rdd = sc.textFile("file:///home/dev/data/accumulator_broadcast_data.txt")
    
    abnormal_char = [",", ".","!", "#", "$", "%"]
    
    broadcast = sc.broadcast(abnormal_char)
    
    count = sc.accumulator(0)
    
    lines_rdd = file_rdd.filter(lambda line: line.strip())
    
    data_rdd = lines_rdd.map(lambda line:line.strip())
    
    words_rdd = data_rdd.flatMap(lambda line: re.split("\s+", line))
    
    def filter_func(data):
        global count
        if data in broadcast.value:
            count += 1
            return False
        else:
            return True
    
    normal_words_rdd = words_rdd.filter(filter_func)
    
    result_rdd = normal_words_rdd.map(lambda x: (x,1)).reduceByKey(lambda a, b: a + b)
    
    print(result_rdd.collect())
    print(count)
    