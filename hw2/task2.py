import pyspark
import argparse
import json
import time
from sonalg import Sonalg

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
    parser.add_argument('--k', type=int, default=10, help='user filter')
    parser.add_argument('--s', type=int, default=10, help='support')
    parser.add_argument('--input_file', type=str, default='./data/user_business.csv', help='the input file')
    parser.add_argument('--output_file', type=str, default='./data/t2.json', help='the output file contains your answers')

    args = parser.parse_args()
    #end given code

    a = time.perf_counter()
    rdd = sc.textFile(args.input_file).map(lambda x: x.split(",")).filter(lambda x: x[0] != "user_id").persist()

    kusers = rdd.map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b).filter(lambda x: x[1] > args.k)
    frdd = rdd.join(kusers).map(lambda x: (x[0], x[1][0]))
    

    user = Sonalg(args.s)
    rdic = user.son(frdd)

    rdic["Runtime"] = time.perf_counter() - a

    out = open(args.output_file, "w")
    out.write(json.dumps(rdic))


