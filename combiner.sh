#!/bin/bash

if [ $# -ne 7 ]; then
    echo "Invalid number of parameters!"
    echo "Usage: . combiner.sh [python file] [k value] [measurement_file_location] [experiment_file_location] [output_location_1] [output_location_2] [output_location_3]"
    exit 1
fi

spark-submit  \
--master yarn-client \
--num-executors 3 \
$1 $2 $3 $4 $5 $6 $7