# coding:utf8

from posixpath import split
from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel
from defs import append_words, context_jieba, extract_user_and_word, filter_words
from operator import add

if __name__ == "__main__":
    conf = SparkConf().setAppName("test")
    sc = SparkContext(conf=conf)
    
    file_rdd = sc.textFile("hdfs://master:9000/inputs/SogouQ.txt")
    
    split_rdd = file_rdd.map(lambda x: x.split("\t"))
    
    split_rdd.persist(StorageLevel.DISK_ONLY)
    
    # key words analysis
    #print(split_rdd.takeSample(True, 3))
    
    context_rdd = split_rdd.map(lambda x: x[2])
    
    words_rdd = context_rdd.flatMap(context_jieba)

    filter_rdd = words_rdd.filter(filter_words)
    
    final_word_rdd= filter_rdd.map(append_words)
    
    result1 = final_word_rdd.reduceByKey(lambda a, b: a + b).\
        sortBy(lambda x: x[1], ascending=False, numPartitions=1).\
        take(5)
        
    print(result1)
    
    user_content_rdd = split_rdd.map(lambda x:(x[1], x[2]))
    
    user_word_with_one_rdd = user_content_rdd.flatMap(extract_user_and_word)
    
    result2 = user_word_with_one_rdd.reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], ascending=False, numPartitions=1).take(5)
    
    print(result2)
    
    time_rdd = split_rdd.map(lambda x: x[0])
    
    hour_with_one_rdd = time_rdd.map(lambda x: (x.split(":")[0], 1))
    
    result3 = hour_with_one_rdd.reduceByKey(add).sortBy(lambda x: x[1], ascending=False, numPartitions=1).collect()
    
    print(result3)