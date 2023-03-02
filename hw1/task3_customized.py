import pyspark
import argparse
import json
#import time

if __name__ == '__main__':

    #given code
    sc_conf = pyspark.SparkConf() \
        .setAppName('task3_customized') \
        .setMaster('local[*]') \
        .set('spark.driver.memory', '8g') \
        .set('spark.executor.memory', '4g')

    sc = pyspark.SparkContext(conf=sc_conf)
    sc.setLogLevel("OFF")
        

    parser = argparse.ArgumentParser(description='A1T3_customized')
    parser.add_argument('--input_file', type=str, default='./data/hw1/review.json', help='the input file')
    parser.add_argument('--output_file', type=str, default='./data/hw1/a1t3_customized', help='the output file contains your answers')
    parser.add_argument('--n_partitions', type=int, default=30, help='amount of partitions to create')
    parser.add_argument('--n', type=int, default=100, help='businesses need more than n reviews')

    args = parser.parse_args()

    reviewrdd = sc.textFile(args.input_file)

    parsedrdd = reviewrdd.map(lambda x: (json.loads(x)["business_id"], 1))

    def partitionme(rdd, n_part):
        #using business id for hash value because it is unique and unlikely to skew to a specific number
        return rdd.partitionBy(n_part, lambda x: hash(x[0]) % n_part)
        
    partitionedrdd = partitionme(parsedrdd, args.n_partitions).persist()
    #a = time.perf_counter()

    finaldict = dict()
    finaldict["n_partitions"] = args.n_partitions

    nitemslist = []
    glommed = partitionedrdd.glom().collect()
    for partition in glommed:
        nitemslist.append(len(partition))

    finaldict["n_items"] = nitemslist

    businessovern = partitionedrdd.reduceByKey(lambda x, y: x + y).filter(lambda x: x[1] > args.n).collect()

    finaldict["result"] = businessovern

    out = open(args.output_file, "w")
    out.write(json.dumps(finaldict))
    out.close()

    #print(time.perf_counter() - a)