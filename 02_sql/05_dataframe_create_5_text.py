from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType


if __name__ == "__main__":
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()
    
    sc = spark.sparkContext
    schema = StructType().add("data", StringType(), nullable=True)
    df = spark.read.format("text").\
        schema(schema=schema).\
        load("file:///home/dev/data/sql/people.txt")
    
    df.printSchema()
    df.show()