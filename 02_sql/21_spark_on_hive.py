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
        config("spark.sql.warehouse.dir", "hdfs://master:9000/user/hive/warehouse").\
        config("hive.metastore.uris", "thrift://master:9083").\
        enableHiveSupport().\
        getOrCreate()
    
    sc = spark.sparkContext
    
    spark.sql("SELECT * FROM student").show()
