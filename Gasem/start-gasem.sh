#!/bin/bash

echo " MONGO-DB START-UP "
sudo service mongod start

echo " HADOOP START-UP "
$HADOOP_DIR/sbin/start-dfs.sh
$HADOOP_DIR/sbin/start-yarn.sh
jps

echo " COMPLETED"
