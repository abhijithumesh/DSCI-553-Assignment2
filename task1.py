import csv
import math
import sys
import time

import itertools as it
from collections import Counter
from pyspark import SparkContext
from operator import add


def set_actually_frequent(possible_set, dict_freq_items):

    for item in possible_set:
        possible_set.remove(item)
        
        if frozenset(possible_set) not in dict_freq_items:
            return False
        
        possible_set.add(item)

    return True


def item_combination(frequent_items, desired_count):

    desired_candidate_set = set()
    
    dict_freq_items = dict.fromkeys(frequent_items)
    length = len(frequent_items)
    
    for i in range(length-1):
        for j in range(i+1, length):
        
            if len(frequent_items[i].intersection(frequent_items[j])) == (desired_count-2):
                
                possible_set = set(frequent_items[i].union(frequent_items[j]))
                
                if len(possible_set) == desired_count:
                    if set_actually_frequent(possible_set, dict_freq_items):
                        desired_candidate_set.add(frozenset(possible_set))
                        
                        
    return list(desired_candidate_set)

    
def count_actual_items(potential_items, baskets, support):

    counter = {}
    for basket in baskets:
        for candidate in potential_items:
        
            if candidate.issubset(basket):
                
                if candidate not in counter:
                    counter[candidate] = 1
                    
                else:
                    counter[candidate] += 1
                    
                    
    return [item for item in counter if counter[item] >= support]


def frequent_bucket_algorithm(baskets, support):

    candidate_items = []
    frequent_items = []
    
    count_items = Counter()
    for basket in baskets:
        count_items.update(basket)
        
    frequent_items = [frozenset((item,)) for item in count_items if count_items[item] >= support]
    
    candidate_items.append(frequent_items)
    
    count = 1
    while len(frequent_items) > 0:
        count += 1
        potential_items = item_combination(frequent_items, count)
        frequent_items = count_actual_items(potential_items, baskets, support)
        candidate_items.append(frequent_items)

    return candidate_items
    

def candidate_find(data_baskets, scaled_threshold):

    baskets = list(data_baskets)
    
    itemsets = frequent_bucket_algorithm(baskets, scaled_threshold)

    for itemsetgroup in itemsets:
        for itemset in itemsetgroup:
            if itemset is not None:
                yield(frozenset(itemset))
                
                
def count_actual(baskets, candidates):

    for basket in baskets:
        for candidate in candidates:
            if candidate.issubset(basket):
                yield (frozenset(candidate), 1)


def get_candidate_string(candidates):

    result_list = []
    result_dict = dict()
    
    if len(candidates) == 0:
        return "\n"
    
    
    for candidate in candidates:
        
        if(len(candidate) not in result_dict):
            result_dict[len(candidate)] = []
        
        result_dict[len(candidate)].append(sorted(candidate))
            
    for key in result_dict:
        result_list.append(sorted(result_dict[key]))
        
    res_str = ""
    for val in result_list:
    
        res_str += ",".join(["(" + ", ".join(["'" + str(ch) + "'" for ch in subset]) + ")"  for subset in val])
        res_str += "\n\n"
        
    return res_str
    

def m_partition1(lines):
    for line in lines:
        line = line.split(",")
        yield (line[0], frozenset((line[1],)))


def m_partition2(lines):
    for line in lines:
        line = line.split(",")
        yield (line[1], frozenset((line[0],)))


if __name__ == "__main__":

    start = time.time()
    
    sc = SparkContext(appName="HW2-SON-task1", master="local[*]")
    sc.setLogLevel("ERROR")
    
    case = int(sys.argv[1])
    support = int(sys.argv[2])
    input_file_path = sys.argv[3]
    output_file = sys.argv[4]

    rdd = sc.textFile(input_file_path)    
    #first_line = rdd.first()
    #rdd = rdd.filter(lambda line: line != first_line)
    
    if case == 1:
        rdd = rdd.mapPartitions(m_partition1, preservesPartitioning=True) \
              .reduceByKey(lambda a,b: frozenset(a.union(b))).values().persist()
        
    elif case == 2:
        rdd = rdd.mapPartitions(m_partition2, preservesPartitioning=True) \
              .reduceByKey(lambda a,b: frozenset(a.union(b))).values().persist()
    
    ps = math.floor(support / rdd.getNumPartitions())
    
    candidates = rdd.mapPartitions(lambda partition: candidate_find(data_baskets=partition, scaled_threshold=ps), preservesPartitioning=True)\
                 .distinct().collect()
                 
    sc.broadcast(candidates)
    
    actuals = rdd.mapPartitions(lambda item1: count_actual(item1, candidates), preservesPartitioning=True). \
               reduceByKey(add).filter(lambda x: x[1] >= support).keys().collect()

    with open(output_file, "w") as fp:
        
        result = "Candidates:"
        
        result += "\n" + get_candidate_string(candidates)
        
        result += "Frequent Itemsets:"
        
        result += "\n" + get_candidate_string(actuals).rstrip()
        
        fp.write(result)

    end = time.time()
    print("Duration: ", end - start)