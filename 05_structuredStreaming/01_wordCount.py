from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql import functions as F

if __name__ == "__main__":
    spark = SparkSession.builder\
            .appName("StructuredNetworkWordCount")\
            .getOrCreate()
        
    spark.sparkContext.setLogLevel('WARN')
    
    lines = spark.readStream\
            .format("socket")\
            .option("host", "localhost")\
            .option("port", 9999)\
            .load()
            
    words = lines.select(F.explode(F.split(lines.value, " ")).alias("word"))
    
    wordCounts = words.groupBy("word").count()
    
    query = wordCounts.writeStream\
            .outputMode("complete")\
            .format("console")\
            .trigger(processingTime="8 seconds")\
            .start()
        
    query.awaitTermination()
    
    # use nc -lp 9999 on terminal to input streaming data
    