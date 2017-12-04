# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 2
# Recipe 2-8.: Install Apache Mesos
# Run following shell commands on centos or any other 

#Step 2-8-1.  Downloading Apache Mesos.

wget http://www.apache.org/dist/mesos/1.1.0/mesos-1.1.0.tar.gz

#Step 2-8-2.  Extracting Mesos form .tar.gz 

tar xvzf mesos-1.1.0.tar.gz

#Step 2-8-3.  Installing Repo to install maven 

sudo bash -c 'cat > /etc/yum.repos.d/wandisco-svn.repo <<EOF'

#Step 2-8-4.  Installing Dependencies of Maven.

sudo yum install -y apache-maven python-devel java-1.8.0-openjdk-devel zlib-devel libcurl-devel openssl-devel cyrus-sasl-devel cyrus-sasl-md5 apr-devel subversion-devel apr-util-devel

#Step 2-8-5.  Downloading Apache Maven

wget http://www-us.apache.org/dist/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz

#Step 2-8-6.  Extracting Maven directory

tar -xvzf apache-maven-3.3.9-bin.tar.gz 
mv apache-maven-3.3.9 /allPySpark/maven
sudo ln -s /allPySpark/maven/bin/mvn /usr/bin/mvn

#Step 2-8-7.  Checking Maven installation.
mvn -version

#Step 2-8-9.  Configuring Mesos

../configure


#Step 2-8-10.  Running Make 
make

#Step 2-8-11.  Running make install

make install

#Step 2-8-12.  Starting Mesos Master 

mesos-master --work_dir=/allPySpark/mesos/workdir 

#Step 2-8-13.  Starting Mesos Slaves
mesos-slave --master=127.0.0.1:5050 --work_dir=/allPySpark/mesos/workdir --systemd_runtime_directory=/allPySpark/mesos/systemd
