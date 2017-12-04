# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 7
# Recipe 7-2 : Implement the K-Nearest Neighbors (KNN) algorithm using PySpark
# Run following PySpark code lines, line by line in PySpark shell

def distanceBetweenTuples(data1 , data2) :
     squaredSum = 0.0
     for i in range(len(data1)):
         squaredSum = squaredSum + (data1[i] - data2[i])**2
     return(squaredSum**0.5)

pythonTuple1 = (1.2, 3.4, 3.2)
pythonTuple2 = (2.4, 2.2, 4.2)
distanceBetweenTuples(pythonTuple1, pythonTuple2)

knnDataList = [((3.09,1.97,3.73),'group1'),
                              ((2.96,2.15,4.16),'group1'),
                              ((2.87,1.93,4.39),'group1'),
                              ((3.02,1.55,4.43),'group1'),
                              ((1.80,3.65,2.08),'group2'),
                              ((1.36,4.43,1.95),'group2'),
                              ((1.71,4.35,1.94),'group2'),
                              ((1.03,3.75,2.12),'group2'),
                              ((2.30,3.59,1.99),'group2')]


knnDataRDD = sc.parallelize(knnDataList, 4)

newRecord = [(2.5, 1.7, 4.2)]
newRecordRDD = sc.parallelize(newRecord, 1)

cartesianDataRDD = knnDataRDD.cartesian(newRecordRDD) 
cartesianDataRDD.take(5)

K = 5

groupAndDistanceRDD = cartesianDataRDD.map(lambda data : (data[0][1] ,distanceBetweenTuples(data[0][0], data[1])))

groupAndDistanceRDD.take(5)
ourClasses = groupAndDistanceRDD.takeOrdered(K, key = lambda data : data[1]) 
ourClasses

ourClassesGroup = [data[0] for data in ourClasses]
ourClassesGroup
max(ourClassesGroup,key=ourClassesGroup.count)

#Step 7-2-1. Creating a function to calculate the distance between two tuples.   
def distanceBetweenTuples(data1 , data2) :
     squaredSum = 0.0
     for i in range(len(data1)):
         squaredSum = squaredSum + (data1[i] - data2[i])**2
     return(squaredSum**0.5)

#Step 7-2-2. Creating a List of given records and transforming it to RDD.
knnDataList = [((3.09,1.97,3.73),'group1'),
                ((2.96,2.15,4.16),'group1'),
                ((2.87,1.93,4.39),'group1'),
                ((3.02,1.55,4.43),'group1'),
                ((1.80,3.65,2.08),'group2'),
                ((1.36,4.43,1.95),'group2'),
                ((1.71,4.35,1.94),'group2'),
                ((1.03,3.75,2.12),'group2'),
                ((2.30,3.59,1.99),'group2')]

K = 5
knnDataRDD = sc.parallelize(knnDataList, 4)

knnDataRDD.take(5)

#Step 7-2-3. Broadcasting the record value.
newRecord = [(2.5, 1.7, 4.2)]
broadCastedValue = sc.broadcast(newRecord)
broadCastedValue.value
broadCastedValue.value[0]

#Step 7-2-4. Broadcasting the record value.

groupAndDistanceRDD = knnDataRDD.map(lambda data : (data[1] ,distanceBetweenTuples(data[0], tuple(broadCastedValue.value[0]))))
groupAndDistanceRDD.take(5)

#Step 7-2-5. Finding the class of new record .

ourClasses = groupAndDistanceRDD.takeOrdered(K, key = lambda data : data[1]) 
ourClasses
ourClassesGroup = [data[0] for data in ourClasses]
ourClassesGroup
max(ourClassesGroup,key=ourClassesGroup.count)



