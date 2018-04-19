#!/bin/bash

if [ $# -ne 4 ]; then
    echo "Invalid number of parameters!"
    echo "Usage: . task1.sh [python file] [measurement_file_location] [experiment_file_location] [task1_output_location]"
    exit 1
fi

spark-submit  \
--master yarn-client \
--num-executors 3 \
$1 $2 $3 $4