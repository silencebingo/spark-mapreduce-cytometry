#!/usr/bin/python
from pyspark import SparkContext
import datetime
import sys

#sample, fsca, ssca,ly6c, cd11b, sca1
def extract_measurement(record):
    try:
        sample, fsca, ssca, cd48, ly6g, cd117, sca1, \
        cd11b, cd150, cd11c, b220, ly6c, cd115, cd135, \
        cd3cd19nk11, cd16cd32, cd45 = record.strip().split(",")
        fsca = float(fsca)
        ssca = float(ssca)
        if 150000 >= fsca >= 1 and 1 <= ssca <= 150000:
            return (sample.strip(), 1)
        else:
            return 0
    except:
        return 0

# def measurement_sample(record):  
#     try:
#         sample, ly6c, cd11b, sca1 = extract_measurement(record)
#         return (sample.strip(), 1)
#     except:
#         return 0

def sum_sample_count(reduced_count, current_count):
    return reduced_count+current_count


#sample, researcher
def extract_experiment(record):
    try:
        sample, date, experiment, day, subject, kind, instrument, researchers = record.strip().split(",")
        researcher_list = researchers.strip().split(";")
        return [(sample, (researcher.strip())) for researcher in researcher_list]
    except:
        return[]


def format_task1(line):
    researcher_name, measurement_data = line
    return ("{}\t{}".format(researcher_name, measurement_data))


if __name__ == "__main__":
    input_measurement = str(sys.argv[1])
    input_experiment = str(sys.argv[2])
    output_task1 = str(sys.argv[3])

    sc = SparkContext(appName="Assignment 02 task1")
    measurement = sc.textFile(input_measurement)
    experiment = sc.textFile(input_experiment)
    
    print("{} -> {}".format(str(datetime.datetime.now()),"Task1 begin!"))
    sample_measurement = measurement.map(extract_measurement).filter(lambda line: line != 0)
    sample_count = sample_measurement.reduceByKey(sum_sample_count,1)
    
    sample_experiment = experiment.filter(lambda line: "LSR-II" in line).flatMap(extract_experiment)

    
    experiment_measurement = sample_experiment.join(sample_count,1).values()#.map(map_to_pair)
    experiment_data = experiment_measurement.reduceByKey(sum_sample_count,1)
    experiment_re = experiment_data.repartition(1)
    task1_output = experiment_re.sortBy(lambda x:(x[1],x[0]), False).map(format_task1)
    
    #measurement.saveAsTextFile("01_measurement")
    #experiment.saveAsTextFile("02_experiment")
    #sample_measurement.saveAsTextFile("03_sample_measurement")
    #sample_count.saveAsTextFile("04_sample_count")
    #sample_experiment.saveAsTextFile("05_sample_experiment")
    #experiment_measurement.saveAsTextFile("06_experiment_measurement")
    #experiment_data.saveAsTextFile("07_experiment_data")
    #experiment_sort.saveAsTextFile("08_experiment_sort")
    task1_output.saveAsTextFile(output_task1)
    print("{} -> {}".format(str(datetime.datetime.now()),"Task1 done!"))