# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 9
# Recipe 9-7.  Apply Ridge Regression.
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

#Step 9-7-4. Creating a linear regression model.

from pyspark.mllib.regression import RidgeRegressionWithSGD  as ridgeSGD
ourModelWithRidge  = ridgeSGD.train(
                                                                       data = autoDataLabelPointTrain, 
                                                                       iterations = 400, 
                                                                       step = 0.0005,
                                                                       regParam = 0.05, 
                                                                       intercept = True
                                                                    )
ourModelWithRidge.intercept
ourModelWithRidge.weights

#Step 9-7-5. Saving the created model.

ourModelWithRidge.save(sc, '/home/pysparkbook/ourModelWithRidge')

from  pyspark.mllib.regression import RidgeRegressionModel as ridgeRegModel

ourModelWithRidgeReloaded = ridgeRegModel.load(sc, '/home/pysparkbook/ourModelWithRidge')
ourModelWithRidgeReloaded.intercept
ourModelWithRidgeReloaded.weights

#Step 9-7-6. Predicting the data using model.

actualDataandRidgePredictedData = autoDataLabelPointTest.map(lambda data : [float(data.label) , float(ourModelWithRidge.predict(data.features))])
actualDataandRidgePredictedData.take(5)

#Step 9-7-7. Evaluating the model we have created.

ourRidgeModelMetrics = rmtrcs(actualDataandRidgePredictedData)
ourRidgeModelMetrics.rootMeanSquaredError
