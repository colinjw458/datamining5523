import pyspark
import argparse
import json
import time
from sonalg import Sonalg

def usertask1():
    user = Sonalg(args.s)
    userresults = user.son(rdd)

    timed = time.perf_counter() - a
    userresults["Runtime"] = timed

    out = open(args.output_file, "w")
    out.write(json.dumps(userresults))

def businesstask1():
    businessver = rdd.map(lambda x: (x[1], x[0]))

    business = Sonalg(args.s)
    businessresults = business.son(businessver)
    
    timed = time.perf_counter() - a
    businessresults["Runtime"] = timed

    out = open(args.output_file, "w")
    out.write(json.dumps(businessresults))


if __name__ == '__main__':

    #given code
    sc_conf = pyspark.SparkConf() \
        .setAppName('task1') \
        .setMaster('local[*]') \
        .set('spark.driver.memory', '8g') \
        .set('spark.executor.memory', '4g')

    sc = pyspark.SparkContext(conf=sc_conf)
    sc.setLogLevel("OFF")
        

    parser = argparse.ArgumentParser(description='A2T1')
    parser.add_argument('--c', type=int, default=2, help='which list')
    parser.add_argument('--s', type=int, default=8, help='support')
    parser.add_argument('--input_file', type=str, default='./data/small2.csv', help='the input file')
    parser.add_argument('--output_file', type=str, default='./data/t1c1s4.json', help='the output file contains your answers')

    args = parser.parse_args()
    #end given code

    a = time.perf_counter()
    rdd = sc.textFile(args.input_file).map(lambda x: x.split(",")).filter(lambda x: x[0] != "user_id")

    if(args.c == 1):
        usertask1()

    elif(args.c == 2):
        businesstask1()

    else:
        print("Bad c value")
        exit()
