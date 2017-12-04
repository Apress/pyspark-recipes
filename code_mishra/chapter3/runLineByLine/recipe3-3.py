# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 3
# Recipe 3-3.   Typecasting data types
# Run following PySpark code lines, line by line in PySpark shell

#Step 3-3-1. Typecast an Integer number 17 to a float.

pythonInt = 17
type(pythonInt)
pythonFloat = float(pythonInt)
print pythonFloat
type(pythonFloat)

#Step 3-3-2. Typecast a String 15 to Integer.

pythonString = "15"
type(pythonString)
pythonInteger = int(pythonString)
type(pythonInteger)

#Step 3-3-3. Typecast a String 15.4 to a float.

pythonString = "15.4"
type(pythonString)
pythonFloat = float(pythonString)
pythonFloat
type(pythonFloat)

