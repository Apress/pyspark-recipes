# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 6
# Recipe 6-10. Reading JSON file.
# Run following PySpark code lines, line by line in PySpark shell

#Step 6-10-1.  Creating a function to parse JSON data.
>>> import json

>>> def jsonParse(dataLine):
...      parsedDict = json.loads(dataLine)
...      valueData = parsedDict.values()
...      return(valueData)

>>> jsonData = '{"Time":"6AM",  "Temperature":15}'
>>> jsonParsedData = jsonParse(jsonData)
>>> print jsonParsedData

#Step 6-10-2.  Reading the File.

>>> tempData = sc.textFile("/home/pysparkbook//pysparkBookData/tempData.json",4)

>>> tempData.take(4)

#Step 6-10-3.  Creating paired RDD.
>>> tempDataParsed = tempData.map(jsonParse)
>>> tempDataParsed.take(4)


