from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("test").\
        master("local[*]").\
        getOrCreate()
    
    sc = spark.sparkContext
    
    df = spark.read.csv("file:///home/dev/data/sql/stu_score.txt", sep=",", header=False)
    df2 = df.toDF("id", "name", "score")
    # df2.printSchema()
    # df2.show()
    
    df2.createTempView("score")
    
    #SQL style
    spark.sql("""
        SELECT * FROM score WHERE name='语文' LIMIT 5
              """).show()
    
    df2.where("name='语文'").limit(5).show()