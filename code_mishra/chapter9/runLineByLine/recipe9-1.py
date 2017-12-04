# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 9
# Recipe 9-1.  Create a Dense Vector.
# Run following PySpark code lines, line by line in PySpark shell

from pyspark.mllib.linalg import DenseVector
denseDataList = [1.0,3.4,4.5,3.2]
denseDataVector = DenseVector(denseDataList)
print denseDataVector
denseDataVector[1]
denseDataVector[0]
denseDataVector[2]
