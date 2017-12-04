# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 2
# Recipe 2-6. Configure Hive metastore on PostgreSQL
# Run following shell commands on centos or any other 

#Step 2-6-1.  Download postgreSQL JDBC connector.

wget https://jdbc.postgresql.org/download/postgresql-9.4.1212.jre6.jar

#Step 2-6-2.   Copy the JDBC connector to hive lib directory 

cp postgresql-9.4.1212.jre6.jar  /allPySpark/hive/lib/

#Step 2-6-3.   Connect to PostgreSQL

sudo -u postgres psql

#Step 2-6-4.  Creating required user and database.

CREATE USER pysparkBookUser WITH PASSWORD 'pbook';
CREATE DATABASE pymetastore;
\c pymetastore; 

#Srep 2-6-5.   Data population in pymetastore database.
\i /allPySpark/hive/scripts/metastore/upgrade/postgres/hive-txn-schema-2.0.0.postgres.sql

#Step 2-6-6.   Granting permissions

grant select, insert,update,delete on public.txns to pysparkBookUser;
grant select, insert,update,delete on public.txn_components to pysparkBookUser;
grant select, insert,update,delete on public.completed_txn_components   to pysparkBookUser;
grant select, insert,update,delete on public.next_txn_id to pysparkBookUser;
grant select, insert,update,delete on public.hive_locks to pysparkBookUser;
grant select, insert,update,delete on public.next_lock_id to pysparkBookUser;
grant select, insert,update,delete on public.compaction_queue to pysparkBookUser;
grant select, insert,update,delete on public.next_compaction_queue_id to pysparkBookUser;
grant select, insert,update,delete on public.completed_compactions to pysparkBookUser;
grant select, insert,update,delete on public.aux_table to pysparkBookUser;

#Step 2-6-7.  Changing  “pg_hba.conf” file 

#Step 2-6-8.  Testing our user 
psql -h localhost -U pysparkbookuser -d pymetastore

#Step 2-6-9. : We have to modify our hive-site.xml

<property>
      <name>javax.jdo.option.ConnectionURL</name>
      <value>jdbc:postgresql://localhost/pymetastore</value>
      <description>postgreSQL server metadata store</description>
 </property>
 <property>
      <name>javax.jdo.option.ConnectionDriverName</name>
      <value>org.postgresql.Driver</value>
      <description>Driver class of postgreSQL</description>
 </property>
  <property>
      <name>javax.jdo.option.ConnectionUserName</name>
      <value>pysparkbookuser</value>
      <description>User name to connect to postgreSQL</description>
 </property>
 <property>
      <name>javax.jdo.option.ConnectionPassword</name>
      <value>pbook</value>
      <description>password for connecting to PostgreSQL server</description>
 </property>

#Step 2-6-10.  We are ready to start hive 

hive



