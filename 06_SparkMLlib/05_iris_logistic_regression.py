from pyspark.ml.linalg import Vector, Vectors
from pyspark.sql import Row, functions
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer, HashingTF, Tokenizer
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel, BinaryLogisticRegressionSummary

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
    
    data.show()
    
    labelIndexer = StringIndexer()\
                    .setInputCol("label")\
                    .setOutputCol("indexedLabel")\
                    .fit(data)
    
    featureIndexer = VectorIndexer()\
                    .setInputCol("features")\
                    .setOutputCol("indexedFeatures")\
                    .fit(data)
    
    lr = LogisticRegression()\
        .setLabelCol("indexedLabel")\
        .setFeaturesCol("indexedFeatures")\
        .setMaxIter(100)\
        .setRegParam(0.3)\
        .setElasticNetParam(0.8)
    
    print("LogisticRegression parameters:\n" + lr.explainParams())
    
    labelConverter = IndexToString()\
                    .setInputCol("prediction")\
                    .setOutputCol("predictedLabel")\
                    .setLabels(labelIndexer.labels)
    
    lrPipeLine = Pipeline()\
                .setStages([labelIndexer, featureIndexer, lr, labelConverter])
    
    trainingData, testData = data.randomSplit([0.7, 0.3])
    
    lrPipelineModel = lrPipeLine.fit(trainingData)
    lrPredictions = lrPipelineModel.transform(testData)
    
    preRel = lrPredictions.select("predictedLabel", "label", "features", "probability").collect()
    
    for item in preRel:
        print(str(item["label"]) + "," + \
            str(item["features"]) + " ==>prob= " +\
            str(item["probability"]) + " ,predictedLabel " + \
            str(item["predictedLabel"]))
    
    evaluator = MulticlassClassificationEvaluator()\
            .setLabelCol("indexedLabel")\
            .setPredictionCol("prediction")
    
    lrAccuracy = evaluator.evaluate(lrPredictions)
    print("testing Accuracy: ", lrAccuracy)
    
    lrModel = lrPipelineModel.stages[2]
    print ("Coefficients: \n " + str(lrModel.coefficientMatrix)+ \
    "\nIntercept: "+str(lrModel.interceptVector)+ \
    "\n numClasses: "+str(lrModel.numClasses)+ \
    "\n numFeatures: "+str(lrModel.numFeatures))