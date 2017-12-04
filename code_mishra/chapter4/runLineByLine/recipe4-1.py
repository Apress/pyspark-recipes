# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 4 
# Recipe 4-1 : Creating a RDD
# Run following PySpark code lines, line by line in PySpark shell

#Step 4-1-1. Create RDD of list.

pythonList = [2.3,3.4,4.3,2.4,2.3,4.0]
pythonList
parPythonData = sc.parallelize(pythonList,2)
parPythonData.collect()

#Step 4-1-2. Getting first element.

parPythonData.first()

#Step 4-1-3. Getting first two elements.

parPythonData.take(2)

#Step 4-1-4. Getting number of partitions in RDD.

parPythonData.getNumPartitions()
