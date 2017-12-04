# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 6 
# Recipe 6-6. Reading Data from Sequential File.
# Run following PySpark code lines, line by line in PySpark shell

>>> simpleRDD = sc.sequenceFile('hdfs://localhost:9746/sequenceFileToRead')
>>> simpleRDD.collect()
