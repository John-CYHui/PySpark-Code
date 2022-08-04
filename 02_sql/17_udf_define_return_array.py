from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType
from pyspark.sql import functions as F

if __name__ == "__main__":
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        config("spark.sql.shuffle.partitions", 2).\
        getOrCreate()
    
    sc = spark.sparkContext
    
    rdd = sc.parallelize([["hadoop spark flink"], ["hadoop flink java"]])
    df = rdd.toDF(["line"])
    
    # register UDF function
    def split_line(data):
        return data.split(" ")
    
    # TODO 1 construct UDF
    udf2 = spark.udf.register("udf1", split_line, ArrayType(StringType()))
    
    # DSL style
    df.select(udf2(df["line"])).show(truncate=False)
    # SQL style
    df.createTempView("lines")
    spark.sql("SELECT udf1(line) FROM lines").show(truncate=False)
    
    # TODO 2 
    udf3 = F.udf(split_line, ArrayType(StringType()))
    df.select(udf3(df["line"])).show(truncate=False)