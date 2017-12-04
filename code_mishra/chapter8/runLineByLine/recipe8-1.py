# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 8
# Recipe 8-1. Creating a DataFrame.
# Run following PySpark code lines, line by line in PySpark shell

#Step 8-1-1. Creating nested list of Filament Data.

filamentData = [['filamentA','100W',605],
...  ['filamentB','100W',683],
...  ['filamentB','100W',691],
...  ['filamentB','200W',561],
...  ['filamentA','200W',530],
...  ['filamentA','100W',619],
...  ['filamentB','100W',686],
...  ['filamentB','200W',600],
...  ['filamentB','100W',696],
...  ['filamentA','200W',579],
...  ['filamentA','200W',520],
...  ['filamentA','100W',622],
...  ['filamentA','100W',668],
...  ['filamentB','200W',569],
...  ['filamentB','200W',555],
...  ['filamentA','200W',541]]

filamentDataRDD = sc.parallelize(filamentData, 4)
filamentDataRDD.take(4)

#Step 8-1-2 . Creating schema of DataFrame.

>>>from pyspark.sql.types import *
FilamentTypeColumn = StructField("FilamentType",StringType(),True)
BulbPowerColumn = StructField("BulbPower",StringType(),True)
LifeInHoursColumn = StructField("LifeInHours",StringType(),True)
FilamentDataFrameSchema = StructType([FilamentTypeColumn, BulbPowerColumn, LifeInHoursColumn])
FilamentDataFrameSchema

#Step 8-1-3.   Creating RDD of Row objects. 

from pyspark.sql import Row
filamentRDDofROWs = filamentDataRDD.map(lambda x :Row(str(x[0]), str(x[1]), str(x[2])))
filamentRDDofROWs.take(4)

#Step 8-1-4.   Creating DataFrame.

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
filamentDataFrameRaw = sqlContext.createDataFrame(filamentRDDofROWs, FilamentDataFrameSchema)
filamentDataFrameRaw.show(4)

#step 8-1-5.  : Printing schema of DataFrame.

filamentDataFrameRaw.printSchema()

#step 8-1-6.   Changing data type of a column.

filamentDataFrame = filamentDataFrameRaw.withColumn('LifeInHours',filamentDataFrameRaw.LifeInHours.cast(FloatType()))
filamentDataFrame.printSchema()
filamentDataFrame.show(5)
filamentDataFrame.columns

#step 8-1-7.   Filtering out data where  BulbPower is 100W.

filamentDataFrame100Watt = filamentDataFrame.filter(filamentDataFrame.BulbPower == '100W')
filamentDataFrame100Watt.show()

#step 8-1-8. Selecting data from DataFrame, where BulbPower is 100 W and  Bulb Life is greater than 650.

filamentData100WGreater650 =filamentDataFrame.filter((filamentDataFrame.BPower == '100W')  & (filamentDataFrame.LifeInHours > 650.0))
filamentData100WGreater650.show()
 


