# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 7
# Recipe 7-5 : Execute a PySpark script in local mode. 
# Run using the command spark-submit - -master local[2] innerJoinInPySpark.py 

from pyspark import SparkContext
studentData = [['si1','Robin','M'],
               ['si2','Maria','F'],
               ['si3','Julie','F'],
               ['si4','Bob',  'M'],
               ['si6','William','M']]

subjectsData = [['si1','Python'],
		['si3','Java'],
		['si1','Java'],
		['si2','Python'],
                ['si3','Ruby'],
                ['si4','C++'],
                ['si5','C'],
                ['si4','Python'],
                ['si2','Java']]
ourSparkContext = SparkContext(appName = 'innerDataJoining')
ourSparkContext.setLogLevel('ERROR')
studentRDD = ourSparkContext.parallelize(studentData, 2)
studentPairedRDD = studentRDD.map(lambda val : (val[0],[val[1],val[2]]))
subjectsPairedRDD = ourSparkContext.parallelize(subjectsData, 2)
studenSubjectsInnerJoin = studentPairedRDD.join(subjectsPairedRDD)
innerJoinedData = studenSubjectsInnerJoin.collect()
print innerJoinedData

