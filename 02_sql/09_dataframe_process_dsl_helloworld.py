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
        schema("id INT, subject STRING, score INT").\
        load("file:///home/dev/data/sql/stu_score.txt")
        
    # Column object
    id_column = df["id"]
    subject_column = df["subject"]
    
    df.select(["id", "subject"]).show()
    df.select("id", "subject").show()
    df.select(id_column, subject_column).show()
    
    # filter API
    df.filter("score < 99").show()
    df.filter(df["score"] < 99).show()
    
    df.where("score < 99").show()
    df.where(df["score"] < 99).show()
    
    # groupby API
    df.groupBy("subject").count().show()
    df.groupBy(df["subject"]).count().show()
    