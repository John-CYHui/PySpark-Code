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
    
    rdd = sc.parallelize([1,2,3,4,5,6,7]).map(lambda x:[x])
    df = rdd.toDF(["num"])
    
    # TODO 1: create udf
    def num_ride_10(num):
        return num * 10
    
    udf1 = spark.udf.register("udf1", num_ride_10, IntegerType())
    
    # SQL style
    df.selectExpr("udf1(num)").show()
    
    # DSL style
    df.select(udf1(df["num"])).show()
    
    # TODO 2: can only use DSL only
    udf3 = F.udf(num_ride_10, IntegerType())
    df.select(udf3(df["num"])).show()