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
        
    df.select(F.concat_ws("---", "user_id", "movie_id", "ts")).\
        write.\
        mode("overwrite").\
        format("text").\
        save("file:///home/dev/data/output/sql/text")
    
    df.write.mode("overwrite").\
        format("csv").\
        option("sep", ";").\
        option("header", True).\
        save("file:///home/dev/data/output/sql/csv")
        
    df.write.mode("overwrite").\
        format("json").\
        save("file:///home/dev/data/output/sql/json")
    
    df.write.mode("overwrite").\
        format("parquet").\
        save("file:///home/dev/data/output/sql/parquet")