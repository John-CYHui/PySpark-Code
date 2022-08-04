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
    rdd = sc.parallelize([1,2,3,4,5],3)
    df = rdd.map(lambda x: [x]).toDF(["num"])
    
    df.show()
    
    single_partiton_rdd = df.rdd.repartition(1)
    
    print(single_partiton_rdd.collect())
    
    def process(iter):
        sum = 0
        for row in iter:
            sum += row["num"]
        return [sum]
    
    print(single_partiton_rdd.mapPartitions(process).collect())