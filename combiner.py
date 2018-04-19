#!/usr/bin/python
from pyspark import SparkContext
import random
import datetime
import sys



"""task1"""
#sample, fsca, ssca,ly6c, cd11b, sca1
def extract_measurement(record):
    try:
        sample, fsca, ssca, cd48, ly6g, cd117, sca1, \
        cd11b, cd150, cd11c, b220, ly6c, cd115, cd135, \
        cd3cd19nk11, cd16cd32, cd45 = record.strip().split(",")
        fsca = float(fsca)
        ssca = float(ssca)
        ly6c = float(ly6c)
        cd11b = float(cd11b)
        sca1 = float(sca1)
        if 150000 >= fsca >= 1 and 1 <= ssca <= 150000:
            return (sample, ly6c, cd11b, sca1)
        else:
            return ()
    except:
        return ()

def measurement_sample(record):  
    try:
        sample, ly6c, cd11b, sca1 = extract_measurement(record)
        return (sample.strip(), 1)
    except:
        return 0

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


"""task2"""
def measurement_sample2(record):
    try:
        sample, ly6c, cd11b, sca1 = extract_measurement(record)
        return (sample.strip(), (ly6c,cd11b,sca1))
    except:
        return 0

def random_centroids(k,ly6c_max,ly6c_min,cd11b_max,cd11b_min,sca1_max,sca1_min):
    centroids_list = []
    ly6c_delta = ly6c_max-ly6c_min
    cd11b_delta = cd11b_max-cd11b_min
    sca1_delta = sca1_max-sca1_min
    for i in range(k):
        rand = random.uniform(0.3, 0.7)
        ly6c = ly6c_min + rand*ly6c_delta
        cd11b = cd11b_min + rand*cd11b_delta
        sca1 = sca1_min + rand*sca1_delta
        centroids_list.append((i,(ly6c, cd11b, sca1)))
    return centroids_list

def construct_centroid(row,k):# k is rdd
    newlist = k #[(1,(ly6c,cd11b,sca1)),(2,(...))...]
    pair = tuple()
    ly6c, cd11b, sca1 = row
    shortest = cal_distance(newlist[0],ly6c,cd11b,sca1)
    pair = (1, (ly6c,cd11b,sca1))
    for i in range(len(newlist)):
        distance = cal_distance(newlist[i],ly6c,cd11b,sca1)
        if distance<=shortest:
            shortest = distance
            pair = (i+1,(ly6c,cd11b,sca1))#not rdd
    return (pair[0],pair[1])#rdd

def cal_distance(newtuple,ly6c,cd11b,sca1):
    c_ly6c,c_cd11b,c_sca1 = newtuple[1][0],newtuple[1][1],newtuple[1][2]
    return ((ly6c-c_ly6c)**2+(cd11b-c_cd11b)**2+(sca1-c_sca1)**2)**0.5

def merge1(accumulated,current_rating):
    a1,b1,c1 = current_rating
    part1,count = accumulated
    a,b,c = part1
    total = (a1+a,b1+b,c1+c)
    count += 1 
    return (total,count)

def merge2(accumulated_pair_1, accumulated_pair_2):
    rating_total_1, rating_count_1 = accumulated_pair_1
    rating_total_2, rating_count_2 = accumulated_pair_2
    a1,b1,c1 = rating_total_1
    a2,b2,c2 = rating_total_2
    total = (a1+a2,b1+b2,c1+c2)
    return (total, rating_count_1+rating_count_2)

def map_to_centroid(line):
    centroid,count = line
    return ((centroid[0]/count,centroid[1]/count,centroid[2]/count),count)

def map_count(line):
    cluster,centroid_count = line
    count = centroid_count[1]
    return (cluster,count)

def map_new_centroid(line):
    cluster,centroid_count = line
    centroid = centroid_count[0]
    return (cluster,centroid)

def format_task2(line):
    clusterid, measurement_all = line
    measurement_data, number_measurement = measurement_all
    data_ly6c, data_cd11b, data_sca1 = measurement_data
    return ("{}\t{}\t{}\t{}\t{}".format(clusterid, number_measurement, data_ly6c, data_cd11b, data_sca1))

"""task3"""

def final_rdd_centroids_distance(record):
    clusterid, measurement_all = record
    measurement_data, centroids_data_all = measurement_all
    measurement_data_ly6c, measurement_data_cd11b, measurement_data_sca1 = measurement_data    
    centroids_data, number_measurement = centroids_data_all
    distance = cal_distance_final(centroids_data,measurement_data_ly6c,measurement_data_cd11b,measurement_data_sca1)
    return(clusterid,((measurement_data_ly6c, measurement_data_cd11b, measurement_data_sca1),distance))

def cal_distance_final(newtuple,ly6c,cd11b,sca1):
    c_ly6c,c_cd11b,c_sca1 = newtuple[0],newtuple[1],newtuple[2]
    return ((ly6c-c_ly6c)**2+(cd11b-c_cd11b)**2+(sca1-c_sca1)**2)**0.5

def count_list(line):
    clusterid, measurement_all = line
    measurement_data, number_measurement = measurement_all
    number_measurement_filter = int(number_measurement*0.9)
    return (clusterid, number_measurement_filter)

def distance_filter(line):
    clusterid, measurement_all = line
    measurement_data, number_measurement = measurement_all
    measurement_data_ly6c, measurement_data_cd11b, measurement_data_sca1 = measurement_data
    return (clusterid, ((measurement_data_ly6c, measurement_data_cd11b, measurement_data_sca1),distance))

def new_dataset(line):
    clusterid, measurement_all = line
    measurement_data, number_measurement = measurement_all
    measurement_data_ly6c, measurement_data_cd11b, measurement_data_sca1 = measurement_data
    return (clusterid, (measurement_data_ly6c, measurement_data_cd11b, measurement_data_sca1))

def new_random_centroids(k,ly6c_max,ly6c_min,cd11b_max,cd11b_min,sca1_max,sca1_min):
    new_centroids_list = []
    new_ly6c_delta = ly6c_max-ly6c_min
    new_cd11b_delta = cd11b_max-cd11b_min
    new_sca1_delta = sca1_max-sca1_min
    for i in range(k):
        rand = random.uniform(0.3, 0.7)
        ly6c = ly6c_min + rand*new_ly6c_delta
        cd11b = cd11b_min + rand*new_cd11b_delta
        sca1 = sca1_min + rand*new_sca1_delta
        new_centroids_list.append((i,(ly6c, cd11b, sca1)))
    return new_centroids_list

"""MAIN"""
if __name__ == "__main__":
    k = int(sys.argv[1])
    input_measurement = str(sys.argv[2])
    input_experiment = str(sys.argv[3])
    output_task1 = str(sys.argv[4])
    output_task2 = str(sys.argv[5])
    output_task3 = str(sys.argv[6])

    sc = SparkContext(appName="Assignment 02 all")
    measurement = sc.textFile(input_measurement)
    experiment = sc.textFile(input_experiment)

    """task1"""
    print("{} -> {}".format(str(datetime.datetime.now()),"Task1 begin!"))
    sample_measurement = measurement.map(measurement_sample).filter(lambda line: line != 0)
    sample_count = sample_measurement.reduceByKey(sum_sample_count)
    
    sample_experiment = experiment.filter(lambda line: "LSR-II" in line).flatMap(extract_experiment)

    
    experiment_measurement = sample_experiment.join(sample_count).values()#.map(map_to_pair)
    experiment_data = experiment_measurement.reduceByKey(sum_sample_count)
    experiment_re = experiment_data.repartition(1)
    task1_output = experiment_re.sortBy(lambda x:(x[1],x[0]), False).map(format_task1)
    
    # measurement.saveAsTextFile("f1_task1_measurement3")
    # experiment.saveAsTextFile("task1_02_experiment")
    # sample_measurement.saveAsTextFile("task1_03_sample_measurement")
    # sample_count.saveAsTextFile("task1_04_sample_count")
    # sample_experiment.saveAsTextFile("task1_05_sample_experiment")
    # experiment_measurement.saveAsTextFile("task1_06_experiment_measurement")
    # experiment_data.saveAsTextFile("task1_07_experiment_data")
    # experiment_sort.saveAsTextFile("task1_08_experiment_sort")
    task1_output.saveAsTextFile(output_task1)
    print("{} -> {}".format(str(datetime.datetime.now()),"Task1 done!"))

    """task2"""
    print("{} -> {}".format(str(datetime.datetime.now()),"Task2 begin!"))
    sample_measurement2 = measurement.map(measurement_sample2).filter(lambda line: line != 0).repartition(3) #(sample.strip(), (ly6c,cd11b,sca1))
    ly6c_rdd = sample_measurement2.values().map(lambda v:v[0])
    cd11b_rdd = sample_measurement2.values().map(lambda v:v[1])
    sca1_rdd = sample_measurement2.values().map(lambda v:v[2])

    ly6c_max,ly6c_min = ly6c_rdd.max(),ly6c_rdd.min()
    cd11b_max,cd11b_min = cd11b_rdd.max(),cd11b_rdd.min()
    sca1_max,sca1_min = sca1_rdd.max(),sca1_rdd.min()

    k = 10

    random_centroids = random_centroids(k,ly6c_max,ly6c_min,cd11b_max,cd11b_min,sca1_max,sca1_min)#list of k tuples

    c_count = sc.parallelize(random_centroids).map(lambda x:(x[0],x[1])).mapValues(lambda v:(v,0))#initialize each cluster to 0 count, (clusterid,((3dimension),count))    

    final_rdd = 0    
    for i in range(10):
        print("{} = {}/{}".format("Task2 Iteration Process",i+1,10))
        new_rdds =sample_measurement2.values().map(lambda row: construct_centroid(row,random_centroids))#rdds (clusterid,(ly6c,cd11b,sca1))  match centroid  
        c_list_with_count = new_rdds.aggregateByKey(((0.0,0.0,0.0),0),merge1,merge2,1).mapValues(map_to_centroid)#cluster,((centroid),count) count
        c_count = c_list_with_count
        random_centroids = c_list_with_count.map(map_new_centroid)
        random_centroids = [x for x in random_centroids.toLocalIterator()] #[(1,(3d),(2,(3d))....)]
        if i == 9:
            final_rdd = new_rdds
    
    c_count_re = c_count.repartition(1)
    task2_output = c_count_re.sortBy(lambda line: line[0]).map(format_task2)
    
    # sample_measurement2.saveAsTextFile("f1_task2_sample_measurement3")
    # new_rdds.saveAsTextFile("task2_02_new_rdds")
    # c_list.saveAsTextFile("task2_03_c_list")
    # c_count.saveAsTextFile("task2_04_count")
    task2_output.saveAsTextFile(output_task2)
    print("{} -> {}".format(str(datetime.datetime.now()),"Task2 done!"))

    """task3"""
    print("{} -> {}".format(str(datetime.datetime.now()),"Task3 begin!"))
    final_rdd_centroids = final_rdd.join(c_count)
    measurement_centroids_distance = final_rdd_centroids.map(final_rdd_centroids_distance)
    count_list_all = c_count.map(count_list)
    count_all = [x for x in count_list_all.toLocalIterator()]
    measurement_centroids_total = measurement_centroids_distance.filter(lambda x: x[0] == 1)
    count_clusterid = count_all[0][1]
    total_distance_list = measurement_centroids_total.takeOrdered(count_clusterid,lambda x: x[1][1])
    total_distance_filter = sc.parallelize(total_distance_list)
    # total_distance_filter.saveAsTextFile("f1_task3_01_total_distance_filter10")
    for i in range(k):
        count_clusterid = count_all[i][1]
        #print(count_clusterid)
        each_measurement_centroids_distance = measurement_centroids_distance.filter(lambda x: x[0] == i+1)#.takeOrdered(count_clusterid)
        each_distance_list = each_measurement_centroids_distance.takeOrdered(count_clusterid,lambda x: x[1][1])
        if i > 0:
            each_distance_filter = sc.parallelize(each_distance_list)
            total_distance_filter = total_distance_filter.union(each_distance_filter)
    
    new_measurement_dataset = total_distance_filter.map(new_dataset).repartition(3)
    
    # new_measurement_dataset.saveAsTextFile("f1_task3_02_new_measurement_dataset9")

    new_ly6c_rdd = new_measurement_dataset.values().map(lambda v:v[0])
    new_cd11b_rdd = new_measurement_dataset.values().map(lambda v:v[1])
    new_sca1_rdd = new_measurement_dataset.values().map(lambda v:v[2])

    new_ly6c_max,new_ly6c_min = new_ly6c_rdd.max(),new_ly6c_rdd.min()
    new_cd11b_max,new_cd11b_min = new_cd11b_rdd.max(),new_cd11b_rdd.min()
    new_sca1_max,new_sca1_min = new_sca1_rdd.max(),new_sca1_rdd.min()

    new_random_centroids_rdd = new_random_centroids(k,new_ly6c_max,new_ly6c_min,new_cd11b_max,new_cd11b_min,new_sca1_max,new_sca1_min)#list of k tuples

    new_c_count = sc.parallelize(new_random_centroids_rdd).map(lambda x:(x[0],x[1])).mapValues(lambda v:(v,0))#initialize each cluster to 0 count, (clusterid,((3dimension),count))    

    task3_new_rdds = 0
    for i in range(10):
        print("{} = {}/{}".format("Task3 Iteration Process",i+1,10))
        task3_new_rdds =new_measurement_dataset.values().map(lambda row: construct_centroid(row,new_random_centroids_rdd))#rdds (clusterid,(ly6c,cd11b,sca1))     
        new_c_list_with_count = task3_new_rdds.aggregateByKey(((0.0,0.0,0.0),0),merge1,merge2,1).mapValues(map_to_centroid)#cluster,((centroid),count)
        new_c_count = new_c_list_with_count
        new_random_centroids_rdd = new_c_list_with_count.map(map_new_centroid)
        new_random_centroids_rdd = [x for x in new_random_centroids_rdd.toLocalIterator()]
    
    new_c_count_re = new_c_count.repartition(1)
    task3_output = new_c_count_re.sortBy(lambda line: line[0]).map(format_task2)

    # final_rdd_centroids.saveAsTextFile("task3_01_final_rdd_centroids")
    # measurement_centroids_distance.saveAsTextFile("task3_02_measurement_centroids_distance")
    # count_list_all.saveAsTextFile("task3_03_count_list_all")
    # measurement_centroids_total.saveAsTextFile("task3_04_measurement_centroids_total")
    # total_distance_filter.saveAsTextFile("task3_05_total_distance_filter2")
    # new_measurement_dataset.saveAsTextFile("task3_06_new_measurement_dataset")
    # new_c_list.saveAsTextFile("task3_07_new_c_list")
    # new_c_count.saveAsTextFile("task3_08_new_c_count")
    task3_output.saveAsTextFile(output_task3)
    print("{} -> {}".format(str(datetime.datetime.now()),"Task3 done!"))
