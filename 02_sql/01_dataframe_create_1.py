from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()
    
    sc = spark.sparkContext
    
    rdd = sc.textFile("file:///home/dev/data/sql/people.txt").\
        map(lambda x: x.split(",")).\
        map(lambda x: (x[0], int(x[1])))
        
    df = spark.createDataFrame(rdd, schema=["name", "age"])
    
    df.printSchema()
    
    df.show(20, False)
    
    df.createOrReplaceTempView("people")
    
    spark.sql("SELECT * FROM people WHERE age < 30").show()
    