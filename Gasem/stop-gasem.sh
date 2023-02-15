#!/bin/bash 

echo " CLOSING MONGO-DB "
sudo service mongod stop

echo " CLOSING HADOOP "
$HADOOP_DIR/sbin/stop-dfs.sh
$HADOOP_DIR/sbin/stop-yarn.sh

echo " COMPLETED"
