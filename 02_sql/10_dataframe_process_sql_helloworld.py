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
    
    # CSV
    df = spark.read.format("csv").\
        option("sep", "\t").\
        option("header", False).\
        option("encoding", "utf-8").\
        schema(schema=schema).\
        load("file:///home/dev/data/sql/u.data")
        
    
    # user average score
    df.groupBy("user_id").\
        avg("rank").\
        withColumnRenamed("avg(rank)", "avg_rank").\
        withColumn("avg_rank", F.round("avg_rank", 2)).\
        orderBy("avg_rank", ascending=False).\
        show()
    
    df.createTempView("movie")
    spark.sql("""
              SELECT movie_id, ROUND(AVG(rank), 2) as avg_rank FROM movie GROUP BY movie_id ORDER BY avg_rank DESC
              """).show()
    
    print(df.where(df["rank"] > df.select(F.avg(df["rank"])).first()["avg(rank)"]).count())
    
    user_id = df.where("rank > 3").\
        groupBy("user_id").\
        count().\
        withColumnRenamed("count", "cnt").\
        orderBy("cnt", ascending=False).\
        limit(1).\
        first()["user_id"]
    
    df.filter(df["user_id"] == user_id).\
        select(F.round(F.avg("rank"), 2)).show()
        
    df.groupBy("user_id").\
        agg(
            F.round(F.avg("rank"), 2).alias("avg_rank"),
            F.min("rank").alias("min_rank"),
            F.max("rank").alias("max_rank")
        ).show()
    
    
    df.groupBy("movie_id").\
        agg(
            F.count("movie_id").alias("cnt"),
            F.round(F.avg("rank"), 2).alias("avg_rank")
        ).where("cnt > 100").\
        orderBy("avg_rank", ascending=False).\
        limit(10).\
        show()