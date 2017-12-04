# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 9
# Recipe 9-8.  Apply Lasso Regression.
# Run following PySpark code lines, line by line in PySpark shell

#Step 9-7-1. Reading csv file Data.

autoDataFrame = spark.read.csv('file:///home/pysparkbook/bData/autoMPGDataModified.csv',header=True, inferSchema = True)
autoDataFrame.show(5)
autoDataFrame.printSchema()
autoDataRDDDict = autoDataFrame.rdd
autoDataRDDDict.take(5)
autoDataRDD = autoDataFrame.rdd.map(list)
autoDataRDD.take(5)

#Step 9-7-2. Creating RDD of Labeled Point.

from pyspark.mllib.regression import LabeledPoint
autoDataLabelPoint = autoDataRDD.map(lambda data : LabeledPoint(data[0],[data[1]/10,data[2],float(data[3])/100,data[4]]))
autoDataLabelPoint.take(5)

#Step 9-7-3. Dividing training and testing data.

autoDataLabelPointSplit = autoDataLabelPoint.randomSplit([0.7,0.3])
autoDataLabelPointTrain = autoDataLabelPointSplit[0]
autoDataLabelPointTest = autoDataLabelPointSplit[1]
autoDataLabelPointTrain.take(5)
autoDataLabelPointTest.take(5)
autoDataLabelPointTest.count()
autoDataLabelPointTrain.count()

#Step 9-8-1. Creating a linear regression model with Lasso.

from pyspark.mllib.regression import LassoWithSGD  as lassoSGD
ourModelWithLasso  = lassoSGD.train(data = autoDataLabelPointTrain, iterations = 400, step = 0.0005,regParam = 0.05, intercept = True)

ourModelWithLasso.intercept

ourModelWithLasso.weights

#Step 9-8-2. Predicting the data using lasso model.

actualDataandLassoPredictedData = autoDataLabelPointTest.map(lambda data : (float(data.label) , float(ourModelWithLasso.predict(data.features))))
actualDataandLassoPredictedData.take(5)

#Step 9-8-3. Evaluating the model we have created.

from pyspark.mllib.evaluation import RegressionMetrics as rmtrcs
ourLassoModelMetrics = rmtrcs(actualDataandLassoPredictedData)
ourLassoModelMetrics.rootMeanSquaredError
