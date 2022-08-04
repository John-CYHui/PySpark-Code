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
    
    schema = StructType().add("user_id", StringType(), nullable=True).\
    add("movie_id", IntegerType(), nullable=True).\
    add("rank", IntegerType(), nullable=True).\
    add("ts", StringType(), nullable=True)
    
    df = spark.read.format("csv").\
        option("sep", "\t").\
        option("header", False).\
        option("encoding", "utf-8").\
        schema(schema=schema).\
        load("file:///home/dev/data/sql/u.data")
        
    df.write.mode("overwrite").\
        format("jdbc").\
        option("url", "jdbc:mysql://master:10000/bigdata?useSSL=false&useUnicode=true").\
        option("dbtable", "movie_data").\
        option("user", "root").\
        option("password", "").\
        save()