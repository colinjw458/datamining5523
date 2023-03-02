import pyspark
import argparse
import json

if __name__ == '__main__':

    #given code
    sc_conf = pyspark.SparkConf() \
        .setAppName('task2') \
        .setMaster('local[*]') \
        .set('spark.driver.memory', '8g') \
        .set('spark.executor.memory', '4g')

    sc = pyspark.SparkContext(conf=sc_conf)
    sc.setLogLevel("OFF")
        

    parser = argparse.ArgumentParser(description='A1T2')
    parser.add_argument('--review_file', type=str, default='./data/hw1/review.json', help='the input file')
    parser.add_argument('--business_file', type=str, default='./data/hw1/business.json', help='the output file contains your answers')
    parser.add_argument('--output_file', type=str, default='./data/hw1/a1t2.json', help='the output file contains your answers')
    parser.add_argument('--n', type=int, default=10, help='top n categoreis with the highest average stars')

    args = parser.parse_args()

    reviewrdd = sc.textFile(args.review_file)

    businessrdd = sc.textFile(args.business_file)

    parsedreview = reviewrdd.map(lambda x: json.loads(x))

    parsedbusiness = businessrdd.map(lambda x: json.loads(x))

    mappedr =  parsedreview.map(lambda x: (x["business_id"], x["stars"]))

    mappedb = parsedbusiness.map(lambda x: (x["business_id"], x["categories"]))
    


    sclsit = mappedr.join(mappedb).map(lambda x: (x[1])).filter(lambda x: x[1] != None).map(lambda x: (x[0], x[1].split(","))).collect()

    categdictcount = dict()
    categdictstars = dict()

    for cstar, categlist in sclsit:
        for categ in categlist:
            if categ[0] == " ":
                categ = categ[1:]
            if categ[-1] == " ":
                categ = categ[:-1]

            categdictcount[categ] = categdictcount.get(categ, 0) + 1
            categdictstars[categ] = categdictstars.get(categ, 0) + cstar

    avglist = []
    for entry in categdictstars:
        avglist.append((entry, categdictstars[entry]/categdictcount[entry]))

    initialsort = sorted(avglist, key=(lambda x: x[0]), reverse=False)
    bestcat = sorted(initialsort, key=(lambda x: x[1]), reverse=True)

    result = {"result": bestcat[:args.n]}

    #output
    out = open(args.output_file, "w")
    out.write(json.dumps(result))
    #printlist = []
    #for i in range(0, args.n - 1):
    #    printlist.append("[\"" + bestcat[i][0] + "\", " + str(bestcat[i][1]) + "], ")
    #printlist[-1] = printlist[-1][:-2]
    
    #out.write("{\"result\": [")
    #for entry in printlist:
    #    out.write(entry)
    #out.write("]}")

    out.close()

    





