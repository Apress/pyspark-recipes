# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 2
# Recipe 3-1.   Working with Python data types
# Run following shell commands on centos or any other 


# Step 2-1-1. Creating a new CentOS user.
su root
adduser pysparkbook
passwd anything

# Adding new user to sudo 
usermod -aG wheel pysparkbook
exit

# Looging to user pysparkbook

su pysparkbook
mkdir binaries
sudo  mkdir /allPySpark

#Step 2-1-3. : Installing Java :

sudo yum install java-1.8.0-openjdk.x86_64
java -version


#Step 2-1-4. : Creating password less logging from pysparkbook 
ssh-keygen -t rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 755 ~/.ssh/authorized_keys
ssh localhost
exit

#Downloading  Hadoop 
cd  binaries
wget https://archive.apache.org/dist/hadoop/core/hadoop-2.6.5/hadoop-2.6.5.tar.gz

#Step 2-1-6. :  Moving Hadoop  binaries to installation directory

tar xvzf hadoop-2.6.5.tar.gz 
sudo mv hadoop-2.6.5   /allPySpark/hadoop

#Step 2-1-7. : Modifying Hadoop environment file
 vim /allPySpark/hadoop/etc/hadoop/hadoop-env.sh 

#After opening the Hadoop environment file, add the following line.
 # The java implementation to use.
#export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.111-2.b15.el7_3.x86_64/jre

vim /allPySpark/hadoop/etc/hadoop/hdfs-site.xml 

#After opening “hdfs-site.xml” we have to put following lines in that file.

<property>
  <name>dfs.name.dir</name>
    <value>file:/allPySpark/hdfs/namenode</value>
    <description>NameNode location</description>
</property>

<property>
  <name>dfs.data.dir</name>
    <value>file:/allPySpark/hdfs/datanode</value>
     <description>DataNode location</description>
</property>

<property>
 <name>dfs.replication</name>
 <value>1</value>
 <description> Number of block replication </description>
</property>

cp /allPySpark/hadoop/etc/hadoop/mapred-site.xml.template /allPySpark/hadoop/etc/hadoop/mapred-site.xml

vim /allPySpark/hadoop/etc/hadoop/mapred-site.xml

 <property>
  <name>mapreduce.framework.name</name>
   <value>yarn</value>
 </property>

#Step 2-1-9. : Updating .bashrc file.

vim  ~/.bashrc  

Add the following lines 
export HADOOP_HOME=/allPySpark/hadoop
export PATH=$PATH:$HADOOP_HOME/sbin
export PATH=$PATH:$HADOOP_HOME/bin
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.111-2.b15.el7_3.x86_64/jre
export PATH=$PATH:$JAVA_HOME/bin

# Sourcing the bashrc file 
source ~/.bashrc 

# Allow hdfs to write on /allPySpark
sudo chmod 755 /allPySpark

#Step 2-1-10. : Running Namenode Format

hdfs namenode -format

#Step 2-1-11. : Starting Hadoop 

/allPySpark/hadoop/sbin/start-all.sh



























































































































































































