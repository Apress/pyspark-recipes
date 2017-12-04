# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 6 
# Recipe 6-5. Saving RDD to  HDFS.
# Run following PySpark code lines, line by line in PySpark shell

#Step 6-5-1.   Counting number of characters in each line of file.

>>> playData = sc.textFile('/home/muser/bData/shakespearePlays.txt',4)
>>> playDataLineLength = playData.map(lambda x : len(x))
>>> playDataLineLength.collect()

#Step 6-5-2.   Saving RDD to HDFS.

>>> playDataLineLength.saveAsTextFile('hdfs://localhost:9746/savedData/')

