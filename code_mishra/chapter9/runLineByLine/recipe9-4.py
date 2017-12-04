# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 9
# Recipe 9-4.  Create a RowMatrix. 
# Run following PySpark code lines, line by line in PySpark shell

dataList  = [[ 94.88,  82.04,  52.57],
              [ 35.85,  26.9 ,   3.63],
              [ 41.76,  69.67,  50.62],
              [ 90.45,  54.66,  64.07]]

dataListRDD = sc.parallelize(dataList,4)
ourRowMatrix = rm(rows = dataListRDD, numRows = 4 , numCols = 3)
ourRowMatrix.numRows()
ourRowMatrix.numCols()
