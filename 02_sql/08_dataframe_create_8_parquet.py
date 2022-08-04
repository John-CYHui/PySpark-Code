from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType


if __name__ == "__main__":
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()
    
    sc = spark.sparkContext

    # CSV
    df = spark.read.format("parquet").\
        load("file:///home/dev/data/sql/users.parquet")
        
    df.printSchema()
    df.show()