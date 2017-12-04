# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 4 
# Recipe 4-3. Doing basic data manipulation on RDD
# Run following PySpark code lines, line by line in PySpark shell

#Step 4-3-1. Making list of given table.

studentMarksData = [["si1","year1",62.08,62.4],
  ["si1","year2",75.94,76.75],
  ["si2","year1",68.26,72.95],
  ["si2","year2",85.49,75.8],
  ["si3","year1",75.08,79.84],
  ["si3","year2",54.98,87.72],
  ["si4","year1",50.03,66.85],
  ["si4","year2",71.26,69.77],
  ["si5","year1",52.74,76.27],
  ["si5","year2",50.39,68.58],
  ["si6","year1",74.86,60.8],
  ["si6","year2",58.29,62.38],
  ["si7","year1",63.95,74.51],
  ["si7","year2",66.69,56.92]]

#Step 4-3-2. Parallelizing the data.
studentMarksDataRDD = sc.parallelize(studentMarksData,4)
studentMarksDataRDD.take(2)

#Step 4-3-3. Calculating average semester wise marks.

studentMarksMean = studentMarksDataRDD.map(lambda x : [x[0],x[1],(x[2]+x[3])/2])
studentMarksMean.take(2)

#Step 4-3-4. Filtering student average marks in second year.

secondYearMarks = studentMarksMean.filter(lambda x : "year2" in x)

secondYearMarks.take(2)

#Step 4-3-5. Top 3 students who has scored highest average marks in second year.

sortedMarksData = secondYearMarks.sortBy(keyfunc = lambda x : -x[2])
sortedMarksData.collect()
 sortedMarksData.take(3)
topThreeStudents = secondYearMarks.takeOrdered(num=3, key = lambda x :-x[2])
topThreeStudents

#Step 4-3-6. Bottom 3 students who has scored lowest average marks in second year.

bottomThreeStudents = secondYearMarks.takeOrdered(num=3, key = lambda x :x[2]])
bottomThreeStudents

#Step 4-3-7. Get all the student who has secured more than 80% average marks in second semester of second year.

moreThan80Marks = secondYearMarks.filter(lambda x : x[2] > 80)
moreThan80Marks.collect()




