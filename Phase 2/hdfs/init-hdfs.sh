#!/bin/bash
set -e

 Wait for HDFS NameNode to be available
 echo "Waiting for NameNode to be available..."
 until nc -z -v -w30 namenode 9870
 do
   echo "Waiting for namenode..."
   sleep 5
 done

# Initialize HDFS directories and set permissions
echo "Creating HDFS directories..."
/usr/bin/hdfs dfs -mkdir -p /user/hdfsaccess
/usr/bin/hdfs dfs -chown hdfsaccess:hdfsaccess /user/hdfsaccess

echo "Setting ACLs for / and /tmp/hadoop-root/dfs/data..."
/usr/bin/hdfs dfs -setfacl -R -m user:hdfsaccess:rw /
/usr/bin/hdfs dfs -setfacl -R -m user:hdfsaccess:rwx /tmp/hadoop-root/dfs/data

