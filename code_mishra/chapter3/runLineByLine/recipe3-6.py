# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 3
# Recipe 3-6.   Working with Set
# Run following PySpark code lines, line by line in PySpark shell

#Step 3-6-1. Creating a Set.

pythonSet = {'Book','Pen','NoteBook','Pencil','Book'}
pythonSet

#Step 3-6-2. Adding a new element to a set.

pythonSet.add("Eraser")
pythonSet

#Step 3-6-3. Operate Union on Sets.

pythonSet1 = {'NoteBook','Pencil','Diary','Marker'}
pythonSet1
pythonSet.union(pythonSet1)

#Step 3-6-4. Operate intersection operation on Sets.

pythonSet.intersection(pythonSet1)
