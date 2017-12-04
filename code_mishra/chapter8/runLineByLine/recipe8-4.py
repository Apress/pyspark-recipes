# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 8
# Recipe 8-4.  Executing SQL and HiveQL query on DataFrame. 
# Run following PySpark code lines, line by line in PySpark shell

#Step 8-4-1 : Creating a temporary view in memory.

>>> censusDataFrame.createOrReplaceTempView("censusDataTable")

#Step 8-4-2 : Selecting age and income column using SQL command.

>>> censusDataAgeIncome = spark.sql('select age, income from censusDataTable limit 5')
>>> censusDataAgeIncome.show()
>>> type(censusDataAgeIncome)

#Step 8-4-3 : Computing education wise average hours per week.

>>> avgHoursPerWeekByEducation = spark.sql("select education, round(avg(`hours-per-week`),2) as averageHoursPerWeek from censusDataTable group by education")
>>> avgHoursPerWeekByEducation.show()

