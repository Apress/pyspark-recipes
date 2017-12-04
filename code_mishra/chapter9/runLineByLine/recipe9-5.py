# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 9
# Recipe 9-5.  Create a Labeled Point. 
# Run following PySpark code lines, line by line in PySpark shell

from pyspark.mllib.regression import LabeledPoint
labeledPointData = [[3.09,1.97,3.73,1],
                                      [2.96,2.15,4.16,1],
                                      [2.87,1.93,4.39,1],
                                      [3.02,1.55,4.43,1],
                                      [1.8,3.65,2.08,2],
                                      [1.36,4.43,1.95,2],
                                      [1.71,4.35,1.94,2],
                                      [1.03,3.75,2.12,2],
                                      [2.3,3.59,1.99,2]]

labeledPointDataRDD = sc.parallelize(labeledPointData, 4)
labeledPointDataRDD.take(4)

labeledPointRDD = labeledPointDataRDD.map(lambda data : LabeledPoint(data[3],data[0:3]))
labeledPointRDD.take(4)
