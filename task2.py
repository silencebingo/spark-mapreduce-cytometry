#!/usr/bin/python
from pyspark import SparkContext
import random
import datetime
import sys



"""task2"""
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
            return (sample.strip(), (ly6c,cd11b,sca1))
        else:
            return 0
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
        centroids_list.append((i+1,(ly6c, cd11b, sca1)))
    return centroids_list

def construct_centroid(row,record):# record is list
    newlist = record #[(1,(ly6c,cd11b,sca1)),(2,(...))...] #centroid
    pair = tuple()
    ly6c, cd11b, sca1 = row #measurement
    shortest = cal_distance(newlist[0],ly6c,cd11b,sca1)
    pair = (1, (ly6c,cd11b,sca1))
    for i in range(len(newlist)):
        distance = cal_distance(newlist[i],ly6c,cd11b,sca1)
        if distance<=shortest:
            shortest = distance
            pair = (i+1,(ly6c,cd11b,sca1))#measurement
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


"""MAIN"""
if __name__ == "__main__":
    k = int(sys.argv[1])
    input_measurement = str(sys.argv[2])
    output_task2 = str(sys.argv[3])
    

    sc = SparkContext(appName="Assignment 02 task2")
    measurement = sc.textFile(input_measurement)

    """task2"""
    print("{} -> {}".format(str(datetime.datetime.now()),"Task2 begin!"))
    sample_measurement2 = measurement.map(extract_measurement).filter(lambda line: line != 0).repartition(3) #(sample.strip(), (ly6c,cd11b,sca1))
    ly6c_rdd = sample_measurement2.values().map(lambda v:v[0])
    cd11b_rdd = sample_measurement2.values().map(lambda v:v[1])
    sca1_rdd = sample_measurement2.values().map(lambda v:v[2])

    ly6c_max,ly6c_min = ly6c_rdd.max(),ly6c_rdd.min()
    cd11b_max,cd11b_min = cd11b_rdd.max(),cd11b_rdd.min()
    sca1_max,sca1_min = sca1_rdd.max(),sca1_rdd.min()

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
