from operator import index
from numpy import maximum
from pandas import Categorical
from pyspark.sql import SparkSession

from pyspark.ml.feature import VectorIndexer
from pyspark.ml.linalg import Vector, Vectors

if __name__ == "__main__":
    spark = SparkSession.builder\
            .master("local[*]")\
            .appName("WordCount").getOrCreate()
    
    df = spark.createDataFrame([ \
    (Vectors.dense(-1.0, 1.0, 1.0),), \
    (Vectors.dense(-1.0, 3.0, 1.0),), \
    (Vectors.dense(0.0, 5.0, 1.0), )], ["features"])
    
    indexer = VectorIndexer(inputCol="features",
                            outputCol="indexed",
                            maxCategories=2)
    
    indexerModel = indexer.fit(df)
    
    categoricalFeatures = indexerModel.categoryMaps.keys()
    
    print("Choose " + str(len(categoricalFeatures)) + \
        " categorical features: " + str(categoricalFeatures))

    indexed = indexerModel.transform(df)
    indexed.show()