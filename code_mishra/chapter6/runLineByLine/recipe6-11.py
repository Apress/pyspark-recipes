# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 6 
# Recipe 6-11. Writing a RDD to a  JSON file.
# Run following PySpark code lines, line by line in PySpark shell

#Step 6-11-1.  Creating a function which takes a list and return a JSON string.

>>> def createJSON(data):
...     dataDict = {}
...     dataDict['Name'] = data[0]
...     dataDict['Age'] = data[1]
...     return(json.dumps(dataDict))

>>> nameAgeList = ['Arun',22]

>>> createJSON(nameAgeList)

#Step 6-11-2.  Saving data in JSON format.

>>> nameAgeData = [['Arun',22],
...                                  ['Bony',35],
...                                  ['Juna',29]]
>>> nameAgeRDD = sc.parallelize(nameAgeData,3)

>>> nameAgeRDD.collect()

>>> nameAgeJSON = nameAgeRDD.map(createJSON)
>>> nameAgeJSON.collect()
>>> nameAgeJSON.saveAsTextFile('/home/pysparkbook/jsonDir/')



