import pyspark
import argparse
import json

if __name__ == '__main__':

    #given code
    sc_conf = pyspark.SparkConf() \
        .setAppName('task1') \
        .setMaster('local[*]') \
        .set('spark.driver.memory', '8g') \
        .set('spark.executor.memory', '4g')

    sc = pyspark.SparkContext(conf=sc_conf)
    sc.setLogLevel("OFF")
        

    parser = argparse.ArgumentParser(description='A1T1')
    parser.add_argument('--input_file', type=str, default='./data/hw1/review.json', help='the input file')
    parser.add_argument('--output_file', type=str, default='./data/hw1/a1t1.json', help='the output file contains your answers')
    parser.add_argument('--stopwords', type=str, default='./data/hw1/stopwords', help='the file contains stopwords')
    parser.add_argument('--y', type=int, default=2018, help='year')
    parser.add_argument('--m', type=int, default=10, help='top m users')
    parser.add_argument('--n', type=int, default=10, help='top n frequent words')

    args = parser.parse_args()

    #input
    rdd = sc.textFile(args.input_file)

    parsedrdd = rdd.map(lambda x: json.loads(x)).persist()

    #opening output
    out = open(args.output_file, "w")

    #task A
    numA = parsedrdd.count()

    #task B
    yearrdd = parsedrdd.map(lambda x: x["date"]).filter(lambda x: str(args.y) in x)

    numB = yearrdd.count()

    #task C
    userrdd = parsedrdd.map(lambda x: (x["user_id"], 1)).persist()

    numC = userrdd.distinct().count()

    #ABC output
    out.write("{" + "\"A\": " + str(numA) + 
    ", \"B\": " + str(numB) + 
    ", \"C\": " + str(numC))

    #task D
    bestUsers = userrdd.reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[1]).collect()

    bestUsersFormatted = []
    for i in range(len(bestUsers) - 1, len(bestUsers) - (args.m + 1), -1):
        user = bestUsers[i][0]
        val = bestUsers[i][1]
        bestUsersFormatted.append("[\""+ str(user) + "\"" + ", " + str(val) + "], ")
    bestUsersFormatted[-1] = bestUsersFormatted[-1][:-2]

    #D output
    out.write(", \"D\": [")
    for line in bestUsersFormatted:
        out.write(line)
    out.write("]")

    #task E
    stopwordfile = open(args.stopwords, "r")
    stopwordread = stopwordfile.read()
    stopwords = stopwordread.split("\n")

    wordsrdd = parsedrdd.map(lambda x: x["text"]).flatMap(lambda review: review.split(" ")).map(lambda word: (word.replace("(", "")\
        .replace("[", "")\
        .replace(",", "")\
        .replace(".", "")\
        .replace("!", "")\
        .replace("?", "")\
        .replace(":", "")\
        .replace(";", "")\
        .replace("]", "")\
        .replace(")", "")\
        .lower(), 1))

    mostfrequentwords = wordsrdd.filter(lambda x: x[0] not in stopwords).reduceByKey(lambda x, y: x + y).collect()
    
    mostfrequentwords = sorted(sorted(mostfrequentwords, key=(lambda x: x[0]), reverse=True), key=(lambda x: x[1]), reverse=False)

    #E out
    freqwordsformatted = []
    for i in range(len(mostfrequentwords) - 1, len(mostfrequentwords) - (args.n + 1), -1) :
        key = mostfrequentwords[i][0]
        if(key == ""):
            i = i - 1
            key = mostfrequentwords[i][0]

        freqwordsformatted.append("\"" + key + "\", ")
    freqwordsformatted[-1] = freqwordsformatted[-1][:-2]

    out.write(", \"E\": [")
    for entry in freqwordsformatted:
        out.write(entry)
    out.write("]}")

    #closing output
    out.close()