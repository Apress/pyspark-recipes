# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 4 
# Recipe 4-4. Doing set operation on RDD.
# Run following PySpark code lines, line by line in PySpark shell

#Step 4-4-1. Creation of list of research data year wise

data2001 = ['RIN1', 'RIN2', 'RIN3', 'RIN4', 'RIN5', 'RIN6', 'RIN7']
data2002 = ['RIN3', 'RIN4', 'RIN7', 'RIN8', 'RIN9']
data2003 = ['RIN4', 'RIN8', 'RIN10', 'RIN11', 'RIN12']

#Step 4-4-2.  Parallelizing the data ( Creation of RDD)

parData2001 = sc.parallelize(data2001,2)
parData2002 = sc.parallelize(data2002,2)
parData2003 = sc.parallelize(data2003,2)

#Step 4-4-3. Number of  different research project initiated in three years?

unionOf20012002 = parData2001.union(parData2002)
unionOf20012002.collect()
allResearchs = unionOf20012002.union(parData2003)
allResearchs.collect()

#Step 4-4-4 : Making sets of distinct data 

allUniqueResearchs = allResearchs.distinct()
allUniqueResearchs.collect()

#Step 4-4-5 : Counting distinct elements 

allResearchs.distinct().count()
parData2001.union(parData2002).union(parData2003).distinct().count()

#Step 4-4-6 : How many projects has been completed in first year. 

firstYearCompletion = parData2001.subtract(parData2002)
firstYearCompletion.collect()

#Step 4-4-7 : How many projects have been completed in first two years.

unionTwoYears = parData2001.union(parData2002)
unionTwoYears.subtract(parData2003).collect()
unionTwoYears.subtract(parData2003).distinct().collect()

#Step 4-4-8: Projects which were started in 2001 and continued upto 2003.
projectsInTwoYear = parData2001.intersection(parData2002)
projectsInTwoYear.collect()
projectsInTwoYear.subtract(parData2003).distinct().collect()
