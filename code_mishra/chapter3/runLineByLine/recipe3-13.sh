# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 3
# Recipe 3-13. Integrating IPython with PySpark.
# Run following PySpark code lines in centos shell

#Step 3-13-1. Install IPython.

sudo pip install ipython

#Step 3-13-2. Integrate PySpark with IPython.

export IPYTHON=1

#Step 3-13-3. Install IPython-Notebook.

sudo pip install ipython[notebook]

#Step 3-13-4. Integrate PySpark with IPython-Notebook.

export IPYTHON_OPTS="notebook" 
export XDG_RUNTIME_DIR=""
