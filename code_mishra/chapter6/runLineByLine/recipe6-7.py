# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 6
# Recipe 6-7. Writing data into a  Sequential File.
# Run following PySpark code lines, line by line in PySpark shell

#Step 6-7-1.  Creating paired RDD.

>>> subjectsData = [('si1','Python'),
...                                ('si3','Java'),
...                                ('si1','Java'),
...                                ('si2','Python'),
...                                ('si3','Ruby'),
...                                ('si4','C++'),
...                                ('si5','C'),
...                                ('si4','Python'),
...                                ('si2','Java')]


>>> subjectsPairedRDD = sc.parallelize(subjectsData, 4)

>>> subjectsPairedRDD.take(4)

#Step 6-7-2.  Saving the RDD as a Sequence file.

>>>subjectsPairedRDD.saveAsSequenceFile('hdfs://localhost:9746/sequenceFiles')
