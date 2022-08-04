from pyspark.sql import SparkSession
import numpy as np

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer, IDF

if __name__ == "__main__":
    spark = SparkSession.builder\
            .master("local[*]")\
            .appName("WordCount").getOrCreate()
            
    sentenceData = spark.createDataFrame([
                    (0, "I heard about Spark and I love Spark"),
                    (0, "I wish Java could use case classes"),
                    (1, "Logistic regression models are neat")
                    ]).toDF("label", "sentence")
    
    tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
    wordsData = tokenizer.transform(sentenceData)
    
    wordsData.show()
    
    # Create raw features from words
    hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=2000)
    featurizdData = hashingTF.transform(wordsData)
    
    featurizdData.select("words", "rawFeatures").show(truncate=False)
    
    # Create IDF
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idfModel = idf.fit(featurizdData)
    
    # Rescale data using trained IDF model
    rescaledData = idfModel.transform(featurizdData)
    
    rescaledData.select("features", "label").show(truncate=False)
    
    