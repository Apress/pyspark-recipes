# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 3
# Recipe 3-7.   Working with Dictionary
# Run following PySpark code lines, line by line in PySpark shell

#Step 3-7-1. Create a dictionary of stationary items, with item id as key and item name as value.

pythonDict = {'item1':'Pencil','item2':'Pen', 'item3':'NoteBook'}
pythonDict

#Step 3-7-2. Index an element using key.

pythonDict['item1']
pythonDict.get('item1')
pythonDict.get('item4')

#Step 3-7-3. Get all the keys.
pythonDict.keys()

#Step 3-7-4. Get all the values.
pythonDict.values()

