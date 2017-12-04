# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 9
# Recipe 9-3.  Create Local Matrices.
# Run following PySpark code lines, line by line in PySpark shell

denseDataList = [1.0,3.4,4.5,3.2]
ourDenseMatrix = localMtrix.dense(numRows = 2, numCols = 2, values= denseDataList)
ourDenseMatrix
ourDenseMatrix.toArray()
sparseDataList = [1.0,3.2]
ourSparseMatrix = localMtrix.sparse(numRows = 2,  numCols = 2, colPtrs = [0,1,2],  rowIndices = [0,1], values = sparseDataList)
ourSparseMatrix.toArray()

