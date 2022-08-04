from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType
from pyspark.sql import functions as F
import string

from sqlalchemy import null

if __name__ == "__main__":
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        config("spark.sql.shuffle.partitions", 2).\
        getOrCreate()
    
    sc = spark.sparkContext

    # Demonstrate how to return a dictionary
    rdd = sc.parallelize([[1],[2],[3]])
    df = rdd.toDF(["num"])
    
    def process(data):
        return {"num": data, "letters": string.ascii_letters[data]}
    
    udf1 = spark.udf.register("udf1", process, StructType().\
        add("num", data_type=IntegerType(), nullable=True).\
        add("letters", data_type=StringType(), nullable=True))
    
    # SQL style
    df.selectExpr("udf1(num)").show(truncate=False)
    
    # DSL style
    df.select(udf1(df["num"])).show(truncate=False)