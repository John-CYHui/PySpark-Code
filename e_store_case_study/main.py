from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

if __name__ == "__main__":
    spark = SparkSession.builder.\
        appName("SparkSQL Example").\
        master("local[*]").\
        config("spark.sql.shuffle.partition", "2").\
        config("spark.sql.warehouse.dir", "hdfs://master:8020/user/hive/warehouse").\
        config("hive.metastore.uris", "thrift://master:9083").\
        enableHiveSupport().\
        getOrCreate()
    
    df = spark.read.format("json").load("file:///home/dev/data/mini.json").\
        dropna(thresh=1, subset=['storeProvince']).\
        filter("storeProvince != 'null'").\
        filter("receivable < 10000").\
        select("storeProvince", "storeID", "receivable", "dateTS", "payType")
    
    # TODO 1
    province_sale_df = df.groupBy("storeProvince").sum("receivable").\
        withColumnRenamed("sum(receivable)", "money").\
        withColumn("money", F.round("money", 2)).\
        orderBy("money", ascending=False)
    
    province_sale_df.show(truncate=False)
    
    # write to mysql
    # province_sale_df.write.mode("overwrite").\
    #     format("jdbc").\
    #     option("url", "jdbc:mysql://master:10000/bigdata?useSSL=false&useUnicode=true&characterEncoding=utf8").\
    #     option("dbtable", "province_sale").\
    #     option("user", "root").\
    #     option("password", "").\
    #     option("encoding", "utf-8").\
    #     save()
    
    # # write to hive
    # province_sale_df.write.mode("overwrite").saveAsTable("default.province_sale", "parquet")
    
    
    # TODO 2
    top3_province_df = province_sale_df.limit(3).select("storeProvince").withColumnRenamed("storeProvince", "top3_province")
    
    top3_province_df_joined = df.join(top3_province_df, on= df['storeProvince'] == top3_province_df["top3_province"])
    
    top3_province_df_joined.persist(StorageLevel.MEMORY_AND_DISK)
    
    # province_hot_store_count_df = top3_province_df_joined.groupBy("storeProvince", "storeID", F.from_unixtime(df["dateTS"].substr(0,10), "yyyy-MM-dd").alias("day")).\
    #     sum("receivable").withColumnRenamed("sum(receivable)", "money").\
    #     filter("money > 1000").\
    #     drop_duplicates(subset=["storeID"]).\
    #     groupBy("storeProvince").count()
    
    # province_hot_store_count_df.write.mode("overwrite").saveAsTable("default.province_hot_store_count", "parquet")
    
    # # TODO 3
    # top3_province_order_avg_df = top3_province_df_joined.groupBy("storeProvince").\
    #     avg("receivable").\
    #     withColumnRenamed("avg(receivable)", "money").\
    #     withColumn("money", F.round("money", 2)).\
    #     orderBy("money", ascending=False)
    
    # top3_province_order_avg_df.write.mode("overwrite").saveAsTable("default.province_order_avg", "parquet")

    # TODO 4
    def udf_func(percent):
        return str(round(percent * 100, 2)) + "%"
    
    my_udf = F.udf(udf_func, StringType())
    
    top3_province_df_joined.createTempView("province_pay")
    pay_df = spark.sql("""
              SELECT storeProvince, payType, (COUNT(payType) / total) AS percent FROM
              (SELECT storeProvince, payType, count(1) OVER(PARTITION BY storeProvince) as total FROM province_pay) AS SUB
              GROUP BY storeProvince, payType, total
              """).withColumn("percent", my_udf("percent"))
    
    pay_df.show()
    pay_df.write.mode("overwrite").saveAsTable("default.pay_type", "parquet")
    
    top3_province_df_joined.unpersist()
    