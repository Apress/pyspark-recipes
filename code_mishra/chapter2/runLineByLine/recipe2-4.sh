# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 2
# Recipe 2-4. Install Hive on a single machine
# Run following shell commands on centos or any other 

#Step 2-4-1.  Downloading Hive 
wget http://www-eu.apache.org/dist/hive/hive-2.0.1/apache-hive-2.0.1-bin.tar.gz

#Step 2-4-2.  Extracting Hive  
tar xvzf   apache-hive-2.0.1-bin.tar.gz

#Step 2-4-3.  Moving extracted  Hive directory 

sudo mv apache-hive-2.0.1-bin /allPySpark/hive

#Step 2-4-4.  Updating hive-site.xml 

mv /allPySpark/hive/conf/hive-default.xml.template /allPySpark/hive/conf/hive-default.xml.templatehive-site.xml

vim /allPySpark/hive/conf/hive-site.xml 


#Paste the following lines in hive-site.xml

<name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:derby:;databaseName=/allPySpark/hive/metastore/metastore_db;create=true</value>

mv /allPySpark/hive/conf/hive-env.sh.template /allPySpark/hive/conf/hive-env.sh
vim  /allPySpark/hive/conf/hive-env.sh 

# Set HADOOP_HOME to point to a specific hadoop install directory
HADOOP_HOME=/allPySpark/hadoop

#Step 2-4-5.  Updating .bashrc file 

vim  ~/.bashrc

#Add the following lines into .bashrc file 

####################Hive Parameters ######################
export HIVE_HOME=/allPySpark/hive
export PATH=$PATH:$HIVE_HOME/bin

source ~/.bashrc

#Step 2-4-6.   Creating datawarehouse directories  of Hive

hadoop fs -mkdir -p /user/hive/warehouse
hadoop fs -mkdir -p /tmp
hadoop fs -chmod g+w /user/hive/warehouse
hadoop fs -chmod g+w /tmp

#Step 2-4-7.   Initiating the metastore database.

schematool -initSchema -dbType derby


#Step 2-4-8.  Checking the hive installation.

 hive




