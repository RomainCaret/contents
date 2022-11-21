# hadoop fs -put CO2.csv input

# pyspark --master "local[2]"
from pyspark import SparkContext

sc = SparkContext("local[2]", "TransformationCO2")

def intoInt(x):
    if x == '-':
        return x
    else:
        return int(x)

def valToMean(x, mean):
    if x == '-':
        return mean
    else:
        return x

co2 = sc.textFile('input/CO2.csv')
rdd0 = co2.map(lambda x: x.replace('Ã  aimant permanent,', 'à aimant permanent'))
rdd1 = rdd0.map(lambda x: x.split(","))
rdd2 = rdd1.map(lambda x: [elem.replace('\xa0', '') for elem in x])
rdd3 = rdd2.map(lambda x: [elem.replace('Ã©', 'é') for elem in x])
rdd4 = rdd3.map(lambda x: [elem.replace('€1', '€') for elem in x])
rdd5 = rdd4.map(lambda x: [elem.replace('€', '') for elem in x])
rdd6 = rdd5.filter(lambda x: x[0] != '')
rdd7 = rdd6.map(lambda x: x[2])
rdd7 = rdd6.map(lambda x: (int(x[0]), x[1], intoInt(x[2]),intoInt(x[3]),intoInt(x[4])))
rddmean = rdd7.map(lambda x: x[2]).filter(lambda x: x != '-')
mean = rddmean.mean()
res = rdd7.map(lambda x: (x[0], x[1], valToMean(x[2], mean), x[3], x[4]))

res.saveAsTextFile('output')

