# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 6 
# Recipe 6-8. Reading Comma Separated File (CSV).
# Run following PySpark code lines, line by line in PySpark shell


#Step 6-8-1.  Writing a Python function to parse CSV lines.

>>> import csv
>>> import  StringIO
>>> def parseCSV(csvRow) :
...      data = StringIO.StringIO(csvRow)
...      dataReader =  csv.reader(data)
...      return(dataReader.next())

>>> csvRow = "p,s,r,p"
>>> parseCSV(csvRow)

#Step 6-8-2.  Creating paired RDD.

>>> filamentRDD =sc.textFile('/home/pysparkbook/pysparkBookData filamentData.csv',4)
>>> filamentRDDCSV = filamentRDD.map(parseCSV)
>>> filamentRDDCSV.take(4)
