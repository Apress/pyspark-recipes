# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 9
# Recipe 9-6.  Apply Linear Regression.
# Run following PySpark code lines, line by line in PySpark shell

#Step 9-6-1. Reading csv file Data.

regressionDataFrame = spark.read.csv('file:///home/pysparkbook/bData/linearRegressionData.csv',header=True, inferSchema = True)
regressionDataFrame.show(5)
regressionDataRDDDict = regressionDataFrame.rdd
regressionDataRDDDict.take(5)
regressionDataRDD = regressionDataFrame.rdd.map(list)
regressionDataRDD.take(5)

#Step 9-6-2. Creating RDD of Labeled Point.

from pyspark.mllib.regression import LabeledPoint
regressionDataLabelPoint = regressionDataRDD.map(lambda data : LabeledPoint(data[0],data[1:4]))

regressionDataLabelPoint.take(5)

#Step 9-6-3. Dividing training and testing data.
regressionLabelPointSplit = regressionDataLabelPoint.randomSplit([0.7,0.3])
regressionLabelPointTrainData = regressionLabelPointSplit[0]
regressionLabelPointTrainData.take(5)
regressionLabelPointTrainData.count()
regressionLabelPointTestData = regressionLabelPointSplit[1]
regressionLabelPointTestData.take(5)
regressionLabelPointTestData.count()

#Step 9-6-4. Creating a linear regression model.

from pyspark.mllib.regression import LinearRegressionWithSGD as lrSGD
ourModelWithLinearRegression  = lrSGD.train(
                                                data = regressionLabelPointTrainData,
                                             iterations = 200,
                                             step = 0.02, 
                                             intercept = True)

ourModelWithLinearRegression.intercept
ourModelWithLinearRegression.weights

#Step 9-6-5. Saving the created model.

ourModelWithLinearRegression.save(sc, '/home/pysparkbook/ourModelWithLinearRegression')
from pyspark.mllib.regression import LinearRegressionModel as linearRegressModel

ourModelWithLinearRegressionReloaded = linearRegressModel.load(sc, '/home/pysparkbook/ourModelWithLinearRegression')
ourModelWithLinearRegressionReloaded.intercept
ourModelWithLinearRegressionReloaded.weights

#Step 9-6-6. Predicting the data using model.

actualDataandLinearRegressionPredictedData = regressionLabelPointTestData.map(lambda data : (float(data.label) , float(ourModelWithLinearRegression.predict(data.features))))
actualDataandLinearRegressionPredictedData.take(5)

#Step 9-6-7. Evaluating the model we have created.

from pyspark.mllib.evaluation import RegressionMetrics as rmtrcs
ourLinearRegressionModelMetrics = rmtrcs(actualDataandLinearRegressionPredictedData)
ourLinearRegressionModelMetrics.rootMeanSquaredError
ourLinearRegressionModelMetrics.r2

ourModelWithLinearRegression  = lrSGD.train(data = regressionLabelPointTrainData,
                                             iterations = 100,
                                             step = 0.05, 
                                             intercept = True)

actualDataandLinearRegressionPredictedData = regressionLabelPointTestData.map(lambda data : (float(data.label) , float(ourModelWithLinearRegression.predict(data.features))))

from pyspark.mllib.evaluation import RegressionMetrics as rmtrcs

ourLinearRegressionModelMetrics = rmtrcs(actualDataandLinearRegressionPredictedData)

ourLinearRegressionModelMetrics.rootMeanSquaredError
ourLinearRegressionModelMetrics.r2
