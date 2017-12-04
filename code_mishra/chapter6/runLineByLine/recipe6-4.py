# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 6 
# Recipe 6-4. Reading  Data from HDFS.
# Run following PySpark code lines, line by line in PySpark shell

>>> filamentdata = sc.textFile('hdfs://localhost:9746/bookData/filamentData.csv',4)

>>> filamentdata.take(4)
