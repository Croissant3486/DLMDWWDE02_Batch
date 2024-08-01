#!/bin/bash
set -e

# Wait for HDFS NameNode to be available
echo "Waiting for NameNode to be available..."
until nc -z -v -w30 namenode 9870
do
  echo "Waiting for namenode..."
  sleep 5
done

echo "Creating HDFS directories..."
hdfs dfs -mkdir -p /hadoop/dfs/temperatureData/