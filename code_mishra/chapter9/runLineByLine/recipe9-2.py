# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 9
# Recipe 9-2.  Create a Sparse Vector.
# Run following PySpark code lines, line by line in PySpark shell

from pyspark.mllib.linalg import SparseVector
sparseDataList = [1.0,3.2]
sparseDataVector = SparseVector(8,[0,7],sparseDataList)
sparseDataVector
sparseDataVector[1]
sparseDataVector[7]
sparseDataVector.numNonzeros()
sparseDataList1 = [3.0,1.4,2.5,1.2]
sparseDataVector1 = SparseVector(8,[0,3,4,6],sparseDataList1)
squaredDistance = sparseDataVector.squared_distance(sparseDataVector1)
squaredDistance
