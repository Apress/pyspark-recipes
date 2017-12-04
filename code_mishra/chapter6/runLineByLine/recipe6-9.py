# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 6
# Recipe 6-9. Writing a RDD to a CSV file.
# Run following PySpark code lines, line by line in PySpark shell

#Step 6-9-1.  Creating a function to convert a list into a string.
>>> import csv
>>> import StringIO
>>> def createCSV(dataList):
...       data = StringIO.StringIO()
...       dataWriter = csv.writer(data,lineterminator='')
...       dataWriter.writerow(dataList)
...       return (data.getvalue())

>>> listData = ['p','q','r','s']

>>> createCSV(listData)

#Step 6-9-2.  Saving data to a file.

>>> simpleData = [['p',20],
...                              ['q',30],
...                              ['r',20],
...                              ['m',25]]

>>> simpleRDD = sc.parallelize(simpleData,4)
>>> simpleRDD.take(4)
simpleRDDLines = simpleRDD.map( createCSV)
simpleRDDLines.take(4)
simpleRDDLines.saveAsTextFile('/home/pysparkbook/csvData/')

