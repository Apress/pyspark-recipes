# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 8
# Recipe 8-3.  Performing aggregation operations on DataFrame.
# Run following PySpark code lines, line by line in PySpark shell


#Step 8-3-1. Creating DataFrame from adult.data file.
censusDataFrame = spark.read.csv('file:///home/pysparkbook/pysparkBookData/adult.data',header=True, inferSchema = True)
censusDataFrame.printSchema()

#Step 8-3-2. Counting the total number of records in DataFrame.

censusDataFrame.count()

#Step 8-3-3. Counting the frequency where salary is greater than 50k and frequency where salary is less than 50k. 

groupedByIncome = censusDataFrame.groupBy('income').count()
groupedByIncome.show()

#Step 8-3-4. Performing summary statistics on numeric columns age, capital-gain, capital-loss and hours-per-week. 

censusDataFrame.describe('age').show()
censusDataFrame.describe('capital-gain').show()
censusDataFrame.describe('capital-loss').show()
censusDataFrame.describe('hours-per-week').show()

#Step 8-3-5 .Find out the mean age of male and female worker from the data.

groupedByGender = censusDataFrame.groupBy('sex')
type(groupedByGender)
groupedByGender.mean('age').show()
groupedByGender.mean('hours-per-week').show()

#Step 8-3-6. Find out that,  salary >50k is more frequent in male or female. 

groupedByGenderIncome = censusDataFrame.groupBy(['income', 'sex'])
groupedByGenderIncome.count().show()

#Step 8-3-7.  Find out the highest paid job.

groupedByOccupationIncome = censusDataFrame.groupBy(['occupation', 'income'])
groupedByOccupationIncome.count().sort(['income','count'], ascending= 0).show(5)



