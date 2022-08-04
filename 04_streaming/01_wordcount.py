from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("SparkStreaming").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    
    ssc = StreamingContext(sc, 3)
    lines = ssc.socketTextStream(hostname="localhost", port=9999)
    words = lines.flatMap(lambda x: x.split(" "))
    word_to_one = words.map(lambda x: (x, 1))
    word_to_count = word_to_one.reduceByKey(lambda a, b: a + b)
    
    word_to_count.pprint()
    
    #ssc.stop()
    ssc.start()
    
    ssc.awaitTermination()
    
    # use nc -lp 9999 on terminal to input streaming data
    
    
    
    