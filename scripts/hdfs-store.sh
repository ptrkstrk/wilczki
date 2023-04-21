#!/bin/bash

data_input_path=$1
data_output_path=$2

if [ -z $data_output_path ]
then
  echo "Some parameter value is empty. Usage: hdfs-store.sh <data_input_path> <data_output_path>"
  exit 1
fi

hdfs dfs -mkdir -p $data_output_path
ls $data_input_path  | awk -v d="$data_input_path/" '{s=(NR==0?ds:s " "d)$0}END{print s}' | awk -v d="$data_output_path" '{s=("hdfs dfs -put -f "$0" " d)}END{system(s)}'
# hdfs dfs -put -f $data_input_path $data_output_path
hdfs dfs -ls $data_output_path
