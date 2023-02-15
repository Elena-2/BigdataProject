#!/bin/bash 

echo " DATABASE IMPORT"
mongoimport --db gasem --collection gasem --jsonArray --drop --file $PWD/input/gasem.json

echo " DEPENDENCIES EXPORT"
sh $HADOOP_DIR/etc/hadoop/hadoop-env.sh

echo " COMPLETED"

