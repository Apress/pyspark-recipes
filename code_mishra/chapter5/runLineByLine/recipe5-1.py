# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 5 
# Recipe 5-1. Creation of Paired RDD.
# Run following PySpark code lines, line by line in PySpark shell

#Step  5-1-1.  Creating a RDD with single elements.

pythonList  =  ['b' , 'd', 'm', 't', 'e', 'u']
RDD1 = sc.parallelize(pythonList,2)
RDD1.collect()

#Step  5-1-2.  Writing a Python method which will take an character as input. It will return 0 if input character is a consonant otherwise 1.

def vowelCheckFunction( data) :
     if data in ['a','e','i','o','u']:
        return 1
     else :
        return 0

vowelCheckFunction('a')
vowelCheckFunction('b')

#Step  5-1-3.  Creating a paired RDD.

RDD2 = RDD1.map( lambda data : (data, vowelCheckFunction(data)))
RDD2.collect()

#Step  5-1-4.  Fetching keys from paired RDD.
RDD2Keys = RDD2.keys()

RDD2Keys.collect()

#Step  5-1-5.  Fetching values from paired RDD.

RDD2Values = RDD2.values()
RDD2Values.collect()




