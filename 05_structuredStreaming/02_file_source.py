from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, StructField, TimestampType
from pyspark.sql import functions as F


TEST_DATA_DIR_SPARK = "file:///home/dev/Code/05_structuredStreaming/data/tmp/testdata"

schema = StructType([
    StructField("eventTime", TimestampType(), True),
    StructField("action", StringType(), True),
    StructField("district", StringType(), True)
])

spark = SparkSession.builder\
    .appName("StructuredEMallPurchaseCount")\
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

lines = spark.readStream\
        .format("json")\
        .schema(schema)\
        .option("maxFilesPerTrigger", 100)\
        .load(TEST_DATA_DIR_SPARK)

windowDuration = '1 minutes'

windowedCounts = lines\
                .filter("action = 'purchase'")\
                .groupBy('district', F.window('eventTime', windowDuration))\
                .count()\
                .sort(F.asc('window'))

query = windowedCounts\
        .writeStream\
        .outputMode("complete")\
        .format("console")\
        .option("truncate", "false")\
        .trigger(processingTime="10 seconds")\
        .start()

print("hi")
query.awaitTermination()

                

