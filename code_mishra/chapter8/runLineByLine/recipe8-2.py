# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 8
# Recipe 8-2.  Doing exploratory data analyses on DataFrame.
# Run following PySpark code lines, line by line in PySpark shell

#Step 8-2-1. Defining the DataFrame schema.

from pyspark.sql.types import *
FilamentTypeColumn = StructField("FilamentType",StringType(),True)
BulbPowerColumn = StructField("BulbPower",StringType(),True)
LifeInHoursColumn = StructField("LifeInHours",DoubleType(),True)
FilamentDataFrameSchema = StructType([FilamentTypeColumn, BulbPowerColumn, LifeInHoursColumn])

#Step 8-2-2. Reading csv file and Creating DataFrame.

filamentDataFrame = spark.read.csv('file:///home/pysparkbook/pysparkBookData/filamentData.csv',header=True, schema = FilamentDataFrameSchema, mode="DROPMALFORMED")
filamentDataFrame.show(5)
filamentDataFrame.printSchema()

#Step 8-2-3. Calculating summary statistics.

dataSummary = filamentDataFrame.describe()
dataSummary.show()

#Step 8-2-4. Counting the frequency of distinct values in filament type categorical data  column

filamentDataFrame.filter(filamentDataFrame.FilamentType == 'filamentA').count()
filamentDataFrame.filter(filamentDataFrame.FilamentType == 'filamentB').count()

#Step 8-2-5. Count the frequency of  distinct values in categorical column of bulb power. 

filamentDataFrame.filter(filamentDataFrame.BulbPower  == '100W').count()
filamentDataFrame.filter(filamentDataFrame.BulbPower  == '200W').count()

#Step 8-2-6. Count the frequency of  distinct values in combination of FilamentType and BulbPower columns. 

filamentDataFrame.filter((filamentDataFrame.FilamentType == 'filamentB') & (filamentDataFrame.BulbPower  == '100W')).count()
filamentDataFrame.filter((filamentDataFrame.FilamentType == 'filamentB') & (filamentDataFrame.BulbPower  == '200W')).count()
filamentDataFrame.filter((filamentDataFrame.FilamentType == 'filamentA') & (filamentDataFrame.BulbPower  == '200W')).count()
filamentDataFrame.filter((filamentDataFrame.FilamentType == 'filamentA') & (filamentDataFrame.BulbPower  == '100W')).count()

