# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 3
# Recipe 3-2. How to work with Python String
# Run following PySpark code lines, line by line in PySpark shell

#Step 3-2-1. Create a String and verify its data type.

pythonString  = "PySpark Recipes"
pythonString
type(pythonString)
pythonString  = 'PySpark Recipes'
pythonString

#Step 3-2-2. Indexing a String.
pythonStr = "Learning PySpark is fun"
pythonStr
pythonStr[9]

#Step 3-2-3. Verify if a  substring lies in a given String.

pythonStr[9]
pythonStr.find("Py")
pythonStr.find("py")

#Step 3-2-4. Check if a String starts with a given substring.

pythonStr.startswith("Learning")

#Step 3-2-5. Check if a String ends with a given substring. 
pythonStr.endswith("fun")


