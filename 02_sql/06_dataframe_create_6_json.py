from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType


if __name__ == "__main__":
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()
    
    sc = spark.sparkContext

    # JSON
    df = spark.read.format("json").load("file:///home/dev/data/sql/people.json")
    df.printSchema()
    df.show()