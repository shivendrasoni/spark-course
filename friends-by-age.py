from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

base_path= "file:///Users/shivendrasoni/Desktop/udemy_spark/data/"

def parseLine(line):
    fields = line.split(',')
    age= int(fields[2])
    friends = int(fields[3])
    return (age, friends)



lines = sc.textFile(base_path+"fakefriends.csv")

rdd = lines.map(parseLine)

friendsSumByAge = rdd.mapValues(lambda x: (x,1)).reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))

average = friendsSumByAge.mapValues(lambda x: x[0]/x[1])

results = average.collect()
print("Display number of friends by age")
print("---------------------------------")

for result in results:
    print(result)

print("---------------------------------")


