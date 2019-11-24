# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 2
# Recipe 2-2. Install Spark on a single machine
# Run following shell commands on centos or any other unix operating system 

#Step 2-2-1.  Downloading Apache Spark 

wget  https://archive.apache.org/dist/spark/spark-2.0.0/spark-2.0.0-bin-hadoop2.6.tgz

#Step 2-2-2.  Extracting  .tgz file of spark 

tar xvzf spark-2.0.0-bin-hadoop2.6.tgz 

#Step 2-2-3.  Moving extracted Spark directory to /allPySpark

sudo mv spark-2.0.0-bin-hadoop2.6   /allPySpark/spark

#Step 2-2-4.  Changing  spark environment file 
cp /allPySpark/spark/conf/spark-env.sh.template /allPySpark/spark/conf/spark-env.sh

vim /allPySpark/spark/conf/spark-env.sh

#Now append following  lines at the end of spark-env.sh 

export HADOOP_CONF_DIR=/allPySpark/hadoop/etc/hadoop/
export SPARK_LOG_DIR=/allPySpark/logSpark/
export SPARK_WORKER_DIR=/tmp/spark
export HIVE_CONF_DIR=/allPySpark/hive/conf

#Step 2-2-5.   Amendment of  .bashrc file 

vim  ~/.bashrc

#Add the following lines in .bashrc file.

export SPARK_HOME=/allPySpark/spark
export PATH=$PATH:$SPARK_HOME/bin

#Source tha bashrc 
source ~/.bashrc

#Step 2-2-6.   Starting the pyspark 
pyspark


