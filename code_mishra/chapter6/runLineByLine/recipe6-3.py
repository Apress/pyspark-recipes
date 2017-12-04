# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 6 
# Recipe 6-3. Reading a Directory.
# Run following PySpark code lines, line by line in PySpark shell

#Step 6-3-1.  Reading a directory using textFile() function.

>>> manyFilePlayData = sc.textFile('/home/pysparkbook/pysparkBookData/manyFiles',4)

>>> manyFilePlayData.collect()

#Step 6-3-2.  Reading a directory using wholeTextFiles() function.

>>> manyFilePlayDataKeyValue = sc.wholeTextFiles('/home/pysparkbook/pysparkBookData/manyFiles',4)
>>> manyFilePlayDataKeyValue.collect()

