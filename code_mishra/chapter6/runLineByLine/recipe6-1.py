# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 6
# Recipe 6-1. Reading Simple File.
# Run following PySpark code lines, line by line in PySpark shell
# You will get data file shakespearePlays.txt in dataFile directory.


#Step 6-1-1.  Reading a text file using textFile() function.

playData = sc.textFile('/home/pysparkbook/pysparkBookData/shakespearePlays.txt',2)
playDataList = playData.collect()
type(playDataList)
playDataList[0:4]

#Step 6-1-2.  Reading a text file using wholeTextFiles() function.

playData = sc.wholeTextFiles('/home/pysparkbook/pysparkBookData/shakespearePlays.txt',2)
playData.keys().collect()
playData.values().collect()

#Step 6-1-3.  Counting number of lines in file.
playData.count()

#Step 6-1-4.  Counting number of characters in each line of file and .

pythonString = "My python"
len(pythonString)
playDataLineLength = playData.map(lambda x : len(x))
playDataLineLength.collect()
totalNumberOfCharacters = playDataLineLength.sum()
totalNumberOfCharacters

