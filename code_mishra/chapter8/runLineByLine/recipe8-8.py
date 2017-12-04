# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 8
# Recipe 8-8. Reading data from Apache Hive.
# Run following PySpark code lines, line by line in PySpark shell

#Step 8-8-1. Creating HiveContext object. 

from pyspark.sql import HiveContext
ourHiveContext = HiveContext(sc)

#Step 8-8-2. Reading table data from Hive. 

FilamentDataFrame = ourHiveContext.table('apress.filamenttable')
FilamentDataFrame.show(5)
