import pyspark
import json
import csv

if __name__ == '__main__':

    #given code
    sc_conf = pyspark.SparkConf() \
        .setAppName('task1') \
        .setMaster('local[*]') \
        .set('spark.driver.memory', '8g') \
        .set('spark.executor.memory', '4g')

    sc = pyspark.SparkContext(conf=sc_conf)
    sc.setLogLevel("OFF")

    rdd1 = sc.textFile("./data/business.json").map(lambda x: json.loads(x))
    rdd2 = sc.textFile("./data/review.json").map(lambda x: json.loads(x))

    filtered1 = rdd1.filter(lambda x: x["state"] == "NV").map(lambda x: (x["business_id"], 0))
    filtered2 = rdd2.map(lambda x: (x["business_id"], x["user_id"]))
    wlist = filtered2.join(filtered1).map(lambda x: (x[1][0], x[0])).collect()

    sublist = [["user_id", "business_id"]] + wlist

    f = open("./data/user_business.csv", "w")

    with f:
        out = csv.writer(f)
        out.writerows(sublist)

