# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 4 
# Recipe 4-2. Converting  Temperature
# Run following PySpark code lines, line by line in PySpark shell

# Step 4-2-1. Parallelizing the data.

tempData = [59,57.2,53.6,55.4,51.8,53.6,55.4]
parTempData = sc.parallelize(tempData,2)
parTempData.collect()

#Step 4-2-2. Converting temperature from Fahrenheit to Centigrade.

def fahrenheitToCentigrade(temperature) :
  centigrade = (temperature-32)*5/9
  return centigrade

fahrenheitToCentigrade(59)
parCentigradeData = parTempData.map(fahrenheitToCentigrade)
parCentigradeData.collect()

#Step 4-2-3. Filtering temperature greater than 13o C.

def tempMoreThanThirteen(temperature):
  return temperature >=13

filteredTemprature = parCentigradeData.filter(tempMoreThanThirteen)
filteredTemprature.collect()
filteredTemprature = parCentigradeData.filter(lambda x : x>=13)
filteredTemprature.collect()

