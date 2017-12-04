# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 8
# Recipe 8-5. Performing data joining on DataFrames.
# Run following PySpark code lines, line by line in PySpark shell
# Start PySpark shell using pyspark --driver-class-path  .ivy2/jars/org.postgresql_postgresql-9.4.1212.jar --packages org.postgresql:postgresql:9.4.1212

#Step 8-5-1 : Reading student data table from PostgreSQL data base..

dbURL="jdbc:postgresql://localhost/pysparkbookdb?user=postgres&password=''"   
studentsDataFrame = spark.read.format('jdbc').options(
                                                           url = dbURL,
                                                           database='pysparkbookdb',
                                                           dbtable='studenttable'
                                              ).load()

studentsDataFrame.show()

from pyspark.sql.functions import trim
studentsDataFrame = studentsDataFrame.select(trim(studentsDataFrame.studentid),trim(studentsDataFrame.name),studentsDataFrame.gender)

studentsDataFrame.show()

studentsDataFrame = studentsDataFrame.withColumnRenamed('trim(studentid)', 'studentID').withColumnRenamed('trim(name)','Name').withColumnRenamed('gender', 'Gender')

studentsDataFrame.printSchema()

studentsDataFrame.show()

#Step 8-5-2 : Reading subject data from a JSON file. 

subjectsDataFrame = sqlContext.read.format("json").load('/home/pysparkbook/pysparkBookData/subjects.json')
subjectsDataFrame.show()
subjectsDataFrame.printSchema()

#Step 8-5-3 : Performing inner join on DataFrames. 

joinedDataInner = subjectsDataFrame.join(studentsDataFrame, subjectsDataFrame.studentID==studentsDataFrame.studentID, how='inner')

joinedDataInner.show()

#Step 8-5-4 : Saving inner joined DataFrame  as JSON file.

joinedDataInner = joinedDataInner.select(subjectsDataFrame.studentID,'subject', 'Name', 'Gender')
joinedDataInner.columns
joinedDataInner.write.format('json').save('/home/muser/innerJoinedTable')

#Step 8-5-5 : Performing left outer join.

joinedDataLeftOuter = subjectsDataFrame.join(studentsDataFrame, subjectsDataFrame.studentID==studentsDataFrame.studentID, how='left_outer')
joinedDataLeftOuter.show()

#Step 8-5-6 : Saving left outer joined DataFrame into PostgreSQL.

joinedDataLeftOuter = joinedDataLeftOuter.select(subjectsDataFrame.studentID,'subject', 'Name', 'Gender')

props = { 'user' : 'postgres', 'password' : '' }
joinedDataLeftOuter.write.jdbc(
                                 url   = dbURL,
                                 table = 'joineddataleftoutertable',
                                 mode  = 'overwrite',
                                 properties = props
                                )

#Step 8-5-7 : Performing right outer join.
joinedDataRightOuter = subjectsDataFrame.join(studentsDataFrame, subjectsDataFrame.studentID==studentsDataFrame.studentID, how='right_outer')
joinedDataRightOuter.show()

#Step 8-5-8 : Performing full outer join. 

joinedDataOuter = subjectsDataFrame.join(studentsDataFrame, subjectsDataFrame.studentID==studentsDataFrame.studentID, how='outer')

joinedDataOuter.show()


