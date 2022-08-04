from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql import functions as F
from sqlalchemy import asc

if __name__ == "__main__":
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        config("spark.sql.shuffle.partitions", 2).\
        getOrCreate()
    
    sc = spark.sparkContext
    
    df = spark.read.format("csv").\
        option("sep", ";").\
        option("header", True).\
        load("file:///home/dev/data/sql/people.csv")
    
    df.dropDuplicates().show()
    
    df.dropDuplicates(['age', 'job']).show()
    
    df.dropna().show()
    
    df.dropna(thresh=3).show()
    
    df.dropna(thresh=2, subset=["name", "age"]).show()
    
    df.fillna("loss").show()
    
    df.fillna("N/A", subset=["job"]).show()
    
    df.fillna({"name": "unknown", "age": 1, "job":"worker"}).show()
    