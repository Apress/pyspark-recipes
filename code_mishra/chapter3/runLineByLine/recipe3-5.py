# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 3
# Recipe 3-5.   Working with Tuple
# Run following PySpark code lines, line by line in PySpark shell

#Step 3-5-1. Creating a Tuple

pythonTuple = (2.0,9,"a",True,"a")
type(pythonTuple)
pythonTuple[2]

#Step 3-5-2. Getting index of an element of a Tuple.

pythonTuple.index('a')

#Step 3-5-3. Counting occurrence of a Tuple element.

pythonTuple.count("a")
