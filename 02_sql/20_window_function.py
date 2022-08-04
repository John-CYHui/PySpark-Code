from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType
from pyspark.sql import functions as F

from sqlalchemy import null

if __name__ == "__main__":
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        config("spark.sql.shuffle.partitions", 2).\
        getOrCreate()
    
    sc = spark.sparkContext

    rdd = sc.parallelize([
    ("张三", "class_1", 99),
    ("李四", "class_2", 35),
    ("王五", "class_3", 57),
    ("赵六", "class_4", 12),
    ("王琦", "class_5", 99),
    ("王佳", "class_1", 90),
    ("哈力", "class_2", 91),
    ("王骑", "class_3", 33),
    ("张凝", "class_4", 55),
    ("王三", "class_5", 11),
    ("王久", "class_1", 66),
    ("王丽", "class_2", 36),
    ("王娟", "class_3", 79),
    ("王军", "class_4", 3),
    ("王俊", "class_5", 90),
    ("王君", "class_1", 11),
    ("王珺", "class_2", 14)
    ])

    schema = StructType().add("name", StringType()).\
        add("class", StringType()).\
        add("score", IntegerType())
    
    df = rdd.toDF(schema)
    
    df.createTempView("stu")
    
    # User OVER window function
    spark.sql("""
              SELECT * , AVG(score) OVER() AS avg_score FROM stu
              """).show()
    
    # RANK over, DENSE_RANK over, ROW_NUMBER over
    spark.sql("""
              SELECT *, ROW_NUMBER() OVER(ORDER BY score DESC) AS row_number_rank,
              DENSE_RANK() OVER(PARTITION BY class ORDER BY score DESC) AS dense_rank,
              RANK() OVER(ORDER BY score) AS rank
              FROM stu
              """).show()
    # NTILE
    spark.sql("""
              SELECT *, NTILE(6) OVER(ORDER BY score DESC) FROM stu
              """).show()