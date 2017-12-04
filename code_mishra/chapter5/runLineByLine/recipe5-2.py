# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 5 
# Recipe 5-2. Data aggregation.
# Run following PySpark code lines, line by line in PySpark shell

#Step  5-2-1.  Creating a RDD with single elements.
filDataSingle = [['filamentA','100W',605],
                 ['filamentB','100W',683],
                 ['filamentB','100W',691],
                 ['filamentB','200W',561],
                 ['filamentA','200W',530],
                 ['filamentA','100W',619],
                 ['filamentB','100W',686],
                 ['filamentB','200W',600],
                 ['filamentB','100W',696],
                 ['filamentA','200W',579],
                 ['filamentA','200W',520],
                 ['filamentA','100W',622],
                 ['filamentA','100W',668],
                 ['filamentB','200W',569],
                 ['filamentB','200W',555],
                 ['filamentA','200W',541]]

filDataSingleRDD = sc.parallelize(filDataSingle,2)

filDataSingleRDD.take(3)


# Step  5-2-2.  Creating paired RDD.
filDataPairedRDD1 = filDataSingleRDD.map(lambda x : (x[0], x[2]))
filDataPairedRDD1.take(4)

#Step  5-2-3.  Mean life in hours, of  bulbs Filament Type wise.
filDataPairedRDD11 = filDataPairedRDD1.map(lambda x : [x[0], [x[1], 1]])
filDataPairedRDD11.take(4)


filDataSumandCount = filDataPairedRDD11.reduceByKey(lambda l1,l2 : [l1[0] + l2[0] ,l1[1]+l2[1]])
filDataSumandCount.collect()
filDataPairedRDD11.count()
filDataPairedRDD11.getNumPartitions()
filDataPairedRDD11.take(5)
filDataSumandCount.collect()
filDataMeanandCount = filDataSumandCount.map( lambda l : [l[0],float(l[1][0])/l[1][1],l[1][1]])
filDataMeanandCount.collect()

#Step  5-2-4.  Mean life in hours, bulb power wise.

filDataPairedRDD2 = filDataSingleRDD.map(lambda x : (x[1], x[2]))

filDataPairedRDD2.take(4)
fillDataPairedRDD22 = filDataPairedRDD2.map( lambda x : (x[0],[x[1],1]))

fillDataPairedRDD22.take(4)

powerSumandCount = fillDataPairedRDD22.reduceByKey(lambda l1,l2 : [l1[0]+l2[0], l1[1]+l2[1]])

powerSumandCount.collect()
meanandCountPowerWise =powerSumandCount.map(lambda val : [val[0],[float(val[1][0])/val[1][1],val[1][1]]])

meanandCountPowerWise.collect()

#Step  5-2-5.  Mean life in hours, of  bulbs filament type wise for each power type. 
filDataSingleRDD.take(4)
filDataComplexKeyData = filDataSingleRDD.map( lambda val : [(val[0], val[1]),val[2]])
filDataComplexKeyData.take(4)
filDataComplexKeyData1 = filDataComplexKeyData.map(lambda val : [val[0] ,[val[1],1]])

filDataComplexKeyData1.take(4)
filDataComplexKeySumCount = filDataComplexKeyData1.reduceByKey(lambda l1,l2 : [l1[0]+l2[0], l1[1]+l2[1]])

filDataComplexKeySumCount.collect()
filDataComplexKeyMeanCount = filDataComplexKeySumCount.map(lambda val : [val[0],[float(val[1][0])/val[1][1],val[1][1]]])

filDataComplexKeyMeanCount.collect()


filDataMeanAndCount = filDataSumandCount.map(lambda val : (val[0] ,[float(val[1][0])/val[1][1],val[1][1]]))
filDataMeanAndCount.collect()



