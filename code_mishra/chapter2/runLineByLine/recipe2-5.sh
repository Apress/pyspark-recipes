# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 2
# Recipe 2-5. Install PostgreSQL
# Run following shell commands on centos or any other 

#Step 2-5-1    Installation of PostgreSQL

 sudo yum install postgresql-server

#Step 2-5-2.  Initializing Database
sudo postgresql-setup initdb

#Step 2-5-3. :  Enabling and starting  the database

sudo systemctl enable postgresql
sudo systemctl start postgresql
sudo -i -u postgres

