from pyspark.ml.classification import DecisionTreeClassificationModel
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.linalg import Vector, Vectors
from pyspark.sql import Row
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder\
            .master("local[*]")\
            .appName("logistic_regression").getOrCreate()
            
    def f(x):
        rel = {}
        rel['features']=Vectors\
            .dense(float(x[0]), float(x[1]), float(x[2]), float(x[3]))
        rel['label'] = str(x[4])
        return rel

    data = spark.sparkContext\
        .textFile("file:///home/dev/Code/06_SparkMLlib/iris.txt")\
        .map(lambda line: line.split(","))\
        .map(lambda p: Row(**f(p)))\
        .toDF()
    
    labelIndexer = StringIndexer()\
                .setInputCol("label")\
                .setOutputCol("indexedLabel")\
                .fit(data)
    
    featureIndexer = VectorIndexer()\
                    .setInputCol("features")\
                    .setOutputCol("indexedFeatures")\
                    .setMaxCategories(4)\
                    .fit(data)
                    
    labelConverter = IndexToString()\
                    .setInputCol("prediction")\
                    .setOutputCol("predictedLabel")\
                    .setLabels(labelIndexer.labels)

    trainingData, testData = data.randomSplit([0.7, 0.3])
    
    dtClassifier = DecisionTreeClassifier()\
                .setLabelCol("indexedLabel")\
                .setFeaturesCol("indexedFeatures")
    
    dtPipeline = Pipeline()\
                .setStages([labelIndexer, featureIndexer, dtClassifier, labelConverter])
    
    dtPipelineModel = dtPipeline.fit(trainingData)
    dtPredictions = dtPipelineModel.transform(trainingData)
    
    dtPredictions.select("predictedLabel", "label", "features").show()
    
    evaluator = MulticlassClassificationEvaluator()\
                .setLabelCol("indexedLabel")\
                .setPredictionCol("prediction")
    
    dtAccuracy = evaluator.evaluate(dtPredictions)
    print("Decision Tree model accuracy: ", dtAccuracy)
    
    treeModelClassifier= dtPipelineModel.stages[2]
    print("Learned classfication tree model:\n" + 
          str(treeModelClassifier.toDebugString))