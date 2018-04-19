#!/bin/bash

if [ $# -ne 5 ]; then
    echo "Invalid number of parameters!"
    echo "Usage: . task3.sh [python file] [k value] [measurement_file_location] [task2_output_location] [task3_output_location]"
    exit 1
fi

spark-submit  \
--master yarn-client \
--num-executors 3 \
$1 $2 $3 $4 $5