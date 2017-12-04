# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 2
# Recipe 2-9.: Install HBase
# Run following shell commands on centos or any other 

#Step 2-9-1.  Obtaining HBase 

wget http://www-eu.apache.org/dist/hbase/stable/hbase-1.2.4-bin.tar.gz

#Step 2-9-2.  Extracting HBase

tar xzf  hbase-1.2.4-bin.tar.gz
sudo mv hbase-1.2.4 /usr/local/hbase

#Step 2-9-3.   Updating HBase environment file 

vim /allPySpark/hbase/conf/hbase-env.sh 

#Paste the following
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.111-2.b15.el7_3.x86_64/

#Step 2-9-4.  Creating HBase directory in HDFS

/allPySpark/hadoop/sbin/start-dfs.sh
/allPySpark/hadoop/sbin/start-yarn.sh
hadoop fs -mkdir /hbase

#Step 2-9-5.   Updating HBase property file and .bashrc 

vim /allPySpark/hbase/conf/hbase-site.xml
Now add the following lines in hbase-site.xml

<property>
<name>hbase:rootdir</name>
<value>hdfs://localhost:9746/hbase</value>
</property>

# Update .bashrc file 

vim ~/.bashrc 
#Add the following lines in .bashrc

export HBASE_HOME=/allPySpark/hbase
export PATH=$PATH:$HBASE_HOME/bin

#source  ~/.bashrc 
source  ~/.bashrc 

#Step 2-9-6.  Starting HBase and HBase shell

/allPySpark/hbase/bin/start-hbase.sh 
/allPySpark/hbase/bin/hbase shell





