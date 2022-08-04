from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType


if __name__ == "__main__":
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()
    
    sc = spark.sparkContext

    # CSV
    df = spark.read.format("csv").\
        option("sep", ";").\
        option("header", True).\
        option("encoding", "utf-8").\
        schema("name STRING, age INT, job STRING").\
        load("file:///home/dev/data/sql/people.csv")
        
    df.printSchema()
    df.show()