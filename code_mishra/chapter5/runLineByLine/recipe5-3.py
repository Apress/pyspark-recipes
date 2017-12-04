# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 5 
# Recipe 5-3. Joining on data. 
# Run following PySpark code lines, line by line in PySpark shell

#Step  5-4-1.  Creating nested list of Student table and nested list of Subjects table.
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

#Step  5-4-2.  Creating a paired RDD of students and subjects .

studentRDD = sc.parallelize(studentData, 2)
studentRDD.take(4)

studentPairedRDD = studentRDD.map(lambda val : (val[0],[val[1],val[2]]))
studentPairedRDD.take(4)



subjectsPairedRDD = sc.parallelize(subjectsData, 2)
subjectsPairedRDD.take(4)

#Step  5-4-3.  Performing inner join on student and subject tables.
studenSubjectsInnerJoin = studentPairedRDD.join(subjectsPairedRDD)
studenSubjectsInnerJoin.collect()

#Step  5-3-4.  Performing left outer join on student and subject tables.

studenSubjectsleftOuterJoin = studentPairedRDD.leftOuterJoin(subjectsPairedRDD)

studenSubjectsleftOuterJoin.collect()

#Step  5-3-5.  Performing right outer join on student and subject tables.

studenSubjectsrightOuterJoin = studentPairedRDD.rightOuterJoin(subjectsPairedRDD)

studenSubjectsrightOuterJoin.collect()

#Step  5-3-6.  Performing full outer join on student and subject tables.

studenSubjectsfullOuterJoin = studentPairedRDD.fullOuterJoin(subjectsPairedRDD)

studenSubjectsfullOuterJoin.collect()


