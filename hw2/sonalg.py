import pyspark
from itertools import combinations

class Sonalg:
    def __init__(self, m):
        self.m = m
        self.total_baskets = 0
        self.canidatelist = []
        self.freqlist = []

    def son(self, rdd):
        resultsdict = dict()

        mainrdd = rdd.groupByKey().mapValues(list).persist()

        self.total_baskets = mainrdd.count()

        canidatelists = mainrdd.map(lambda x: x[1]).mapPartitions(self.aprio).collect()
        
        
        for lst in canidatelists:
            for canidate in lst:
                if(len(canidate) > len(self.canidatelist)):
                    for diff in range(len(canidate) - len(self.canidatelist)):
                        self.canidatelist.append([])
                if canidate not in self.canidatelist[len(canidate) - 1]:
                    self.canidatelist[len(canidate) - 1].append(canidate)

        for lst in self.canidatelist:
            for i in range(len(lst[0]) - 1, -1, -1):
                lst.sort(key=(lambda x: x[i]))

        resultsdict["Candidates"] = self.canidatelist

        freqlst = mainrdd.mapPartitions(self.__phase2).reduceByKey(lambda a, b: a + b).filter(lambda x: not x[1] < self.m).map(lambda x: x[0]).collect()

        for item in freqlst:
            if(len(item) > len(self.freqlist)):
                for diff in range(len(item) - len(self.freqlist)):
                    self.freqlist.append([])
            if item not in self.freqlist[len(item) - 1]:
                self.freqlist[len(item) - 1].append(item)

        
        for lst in self.freqlist:
            for i in range(len(lst[0]) - 1, -1, -1):
                lst.sort(key=(lambda x: x[i]))

        resultsdict["Frequent Itemsets"] = self.freqlist

        return resultsdict
        
    def __phase2(self, itemlist):
        returnlist = []
        itemlist = list(itemlist)

        for x, y in itemlist:
            filterlst = []
            for item in y:
                if tuple(sorted(combinations([item], 1)))[0] in self.canidatelist[0]: 
                    filterlst.append(item)
            for j in range(1, len(self.canidatelist[-1][0])):
                for combo in combinations(filterlst, j):
                    if tuple(sorted(combo)) in self.canidatelist[j - 1]:
                        returnlist.append((tuple(sorted(combo)), 1))
                

        return returnlist

    def aprio(self, itemlist):
        hashn = 30000000
        returnlist = []
        canidates = dict()
        freqitems = []
        iteration = 1
        keepGoing = True
        itemlist = list(itemlist)
        s = self.m * (len(itemlist) / self.total_baskets)

        for basket in itemlist:
            for item in basket:
                freqitems.append(item)
                canidates[tuple([item])] = 0
                
                
        
        while(keepGoing):
        
            returnlist.append([])
            newtable = dict()
            hashtable = dict()
            

            for basket in itemlist:
                singles = []
                for item in basket:
                    if item in freqitems:
                        singles.append(item)

                for combo in combinations(singles, iteration):
                    if tuple(sorted(combo)) in canidates:
                        newtable[tuple(sorted(combo))] = newtable.get(tuple(sorted(combo)), 0) + 1
                
                for combo in combinations(singles, iteration + 1):
                    nethash = 0
                    for item in combo:
                        nethash += hash(item)
                    hashtable[nethash % hashn] = hashtable.get(nethash % hashn, 0) + 1

            deletekeys = []
            for key, value in newtable.items():
                        if(value < s):
                            deletekeys.append(key)

            for key in deletekeys:
                newtable.pop(key)

            canidates = dict()
            newset = set()
            for key in newtable:
                returnlist[iteration - 1].append(key)
                newset = newset.union(set(key))

            for key, value in hashtable.items():
                if s < value:
                    hashtable[key] = 1
                else:
                    hashtable[key] = 0
                    
            for combo in combinations(newset, iteration + 1):
                nethash = 0
                for item in combo:
                    nethash += hash(item)
                if(hashtable.get(nethash % hashn, 0)):
                    canUse = True
                    for subcombo in combinations(combo, iteration):
                        if tuple(sorted(subcombo)) not in newtable:
                            canUse = False
                    if(canUse):
                        if tuple(sorted(combo)) not in canidates:
                            canidates[(tuple(sorted(combo)))] = 1

            print(canidates)
                        
            if(returnlist[iteration - 1] == []):
                keepGoing = False
                break

            iteration += 1
        
        returnlist.pop(-1)

        return returnlist
