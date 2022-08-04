from operator import index
from pyspark.sql import SparkSession

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import StringIndexer, IndexToString

if __name__ == "__main__":
    spark = SparkSession.builder\
            .master("local[*]")\
            .appName("WordCount").getOrCreate()
    
    df = spark.createDataFrame([
        (0, "a"),
        (1, "b"),
        (2, "c"),
        (3, "a"),
        (4, "a"),
        (5, "c")
    ], ["id", "category"])
    
    indexer = StringIndexer(inputCol="category", outputCol="categoryIndex")

    # fit the index model
    model = indexer.fit(df)
    
    indexed = model.transform(df)
    indexed.show()
    
    toString = IndexToString(inputCol="categoryIndex", outputCol="originalCategory")
    indexString = toString.transform(indexed)
    
    indexString.select("id", "originalCategory").show()
    