#!/usr/bin/python
from pyspark import SparkContext
import random
import datetime
import sys



"""task3"""
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

def construct_centroid(row,line):# k is rdd
    newlist = line #[(1,(ly6c,cd11b,sca1)),(2,(...))...]
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



def construct_centroid_task3(row,record): # record is list
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
    return (pair[0],(pair[1],distance))#rdd

def cal_distance(newtuple,ly6c,cd11b,sca1):
    c_ly6c,c_cd11b,c_sca1 = newtuple[1][0],newtuple[1][1],newtuple[1][2]
    return ((ly6c-c_ly6c)**2+(cd11b-c_cd11b)**2+(sca1-c_sca1)**2)**0.5




def format_centroid(line):
    cluster_id, measurement_count, ly6c, cd11b, sca1 = line.strip().split("\t")
    ly6c = float(ly6c)
    cd11b = float(cd11b)
    sca1 = float(sca1)
    return(cluster_id,(ly6c, cd11b, sca1))


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
    cluster_id, measurement_count, ly6c, cd11b, sca1 = line.strip().split("\t")
    measurement_count = float(measurement_count)*0.9
    measurement_filter = int(measurement_count)
    return(cluster_id,measurement_filter)

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
        rand = random.uniform(0.4, 0.6)
        ly6c = ly6c_min + rand*new_ly6c_delta
        cd11b = cd11b_min + rand*new_cd11b_delta
        sca1 = sca1_min + rand*new_sca1_delta
        new_centroids_list.append((i+1,(ly6c, cd11b, sca1)))
    return new_centroids_list

def format_task2(line):
    clusterid, measurement_all = line
    measurement_data, number_measurement = measurement_all
    data_ly6c, data_cd11b, data_sca1 = measurement_data
    return ("{}\t{}\t{}\t{}\t{}".format(clusterid, number_measurement, data_ly6c, data_cd11b, data_sca1))

"""MAIN"""
if __name__ == "__main__":
    k = int(sys.argv[1])
    input_measurement = str(sys.argv[2])
    input_centroid = str(sys.argv[3])
    output_task3 = str(sys.argv[4])

    sc = SparkContext(appName="Assignment 02 task3")
    measurement = sc.textFile(input_measurement)
    centroid_input = sc.textFile(input_centroid)

    """task3"""
    print("{} -> {}".format(str(datetime.datetime.now()),"Task3 begin!"))

    centroids_task2 = centroid_input.map(format_centroid) #centroid from task2 [(cluster,((3d))]
    centroids_task2_list = [x for x in centroids_task2.toLocalIterator()]

    sample_measurement3 = measurement.map(extract_measurement).filter(lambda line: line != 0).repartition(3)
    measurement_centroids_distance = sample_measurement3.values().map(lambda row: construct_centroid_task3(row,centroids_task2_list)) # [(cluster,((3d),distance))]
    
    count_list_all = centroid_input.map(count_list)
    count_all = [x for x in count_list_all.toLocalIterator()]
    count_clusterid = count_all[0][1]
    
    measurement_centroids_total = measurement_centroids_distance.filter(lambda x: x[0] == 1)
    total_distance_list = measurement_centroids_total.takeOrdered(count_clusterid,lambda x: x[1][1])
    total_distance_filter = sc.parallelize(total_distance_list)
    # total_distance_filter.saveAsTextFile("f1_task3_01_total_distance_filter10")
    for i in range(k):
        count_clusterid = count_all[i][1]
        # print(count_clusterid)
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
