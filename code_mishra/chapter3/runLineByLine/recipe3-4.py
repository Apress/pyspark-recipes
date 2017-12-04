# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 3
# Recipe 3-4.   How to Work with List
# Run following PySpark code lines, line by line in PySpark shell

#Step 3-4-1. Creating a List

pythonList = [2.3,3.4,4.3,2.4,2.3,4.0]
pythonList
pythonList[0]
pythonList[1]
pythonList1 = ["a",1]
pythonList1
type(pythonList1)
type (pythonList1[0])
type (pythonList1[1])
pythonList1 = ["a",1]
pythonList1[0] = 5
print  pythonList1

#Step 3-4-2. Extending a List

pythonList1 = [5,1]
print  pythonList1
pythonList1.extend([1,2,3])
pythonList1

#Step 3-4-3. Appending a List

pythonList1 = [5,1]
pythonList1.append([1,2,3])
print  pythonList1

#Step 3-4-4. Counting number of Elements in List.

pythonList1 = [5,1]
len(pythonList1)

#Step 3-4-5. Sorting a List

pythonList = [2.3,3.4,4.3,2.4,2.3,4.0]
pythonList
pythonList.sort()
pythonList
pythonList.sort(reverse=True)
pythonList

