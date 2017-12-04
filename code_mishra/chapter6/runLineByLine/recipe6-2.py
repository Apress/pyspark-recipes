# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 6 
# Recipe 6-2. Writing a RDD to Simple File.
# Run following PySpark code lines, line by line in PySpark shell


#Step 6-2-1.   Counting number of characters in each line of file.

>>> playData = sc.textFile('/home/pysparkbook/pysparkBookData/shakespearePlays.txt',4)
>>> playDataLineLength = playData.map(lambda x : len(x))
>>> playDataLineLength.collect()

#Step 6-2-2.  Saving the RDD in file.

>>> playDataLineLength.saveAsTextFile('/home/pysparkbook/savedData')
