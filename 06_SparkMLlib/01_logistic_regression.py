from pyspark.sql import SparkSession
import numpy as np

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer

if __name__ == "__main__":
    spark = SparkSession.builder\
            .master("local[*]")\
            .appName("WordCount").getOrCreate()
    
    # Prepare training dataset from a list of (id, text, label) tuples.
    training = spark.createDataFrame([
            (0, "a b c d e spark", 1.0),
            (1, "b d", 0.0),
            (2, "spark f g h", 1.0),
            (3, "hadoop mapreduce", 0.0)
    ], ["id", "text", "label"])
    
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="features")
    lr = LogisticRegression(maxIter=10, regParam=0.001)
    
    # Create pipeline
    pipeline = Pipeline(stages=[tokenizer, hashingTF, lr])
    
    # Fit model
    model = pipeline.fit(training)
    
    # Prepare test dataset
    test= spark.createDataFrame([
            (4, "spark i j k"),
            (5, "l m n"),
            (6, "spark hadoop spark"),
            (7, "apache hadoop")
    ], ["id", "text"])
    
    # Prediction
    prediction = model.transform(test)
    selected = prediction.select("id", "text", "probability", "prediction")
    
    for row in selected.collect():
        rid, text, prob, prediction = row
        print(f"({rid}, {text}) ==> prob = {prob}, prediction = {prediction}")
    
    
    