from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import time

if __name__ == "__main__":
    conf = SparkConf().setAppName("SparkStreaming").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    
    ssc = StreamingContext(sc, 3)
    
    rddQueue = list()
    # Simulated the queue
    for _ in range(5):
        rddQueue.append(ssc.sparkContext.parallelize(list(range(300)), numSlices=10))

    inputStream = ssc.queueStream(rddQueue, oneAtATime=True)
    
    mappedStream = inputStream.map(lambda x : (x, 1))
    
    reducedStream = mappedStream.reduceByKey(lambda a, b: a + b)
    
    reducedStream.pprint()
    
    ssc.start()
    

    
    ssc.awaitTermination()