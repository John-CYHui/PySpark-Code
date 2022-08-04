from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
import pandas as pd

if __name__ == "__main__":
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()
    
    sc = spark.sparkContext
    
    pdf = pd.DataFrame(
        {
            "id": [1,2,3],
            "name":["Sam", "John", "Joe"],
            "age":[11,21,11]
        }
    )
    
    df = spark.createDataFrame(pdf)
    df.printSchema()
    df.show()