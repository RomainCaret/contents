# hadoop fs -put CO2.csv input

# pyspark --master "local[2]"

from pyspark import SparkContext

sc = SparkContext("local[2]", "TransformationCO2")

# Function used to replace missing values and problematic values
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

# Load the data
co2 = sc.textFile('input/CO2.csv')
rdd0 = co2.map(lambda x: x.split(","))

# 1. Replace the problematic characters 
rdd1 = rdd0.map(lambda x: [elem.replace('Ã', 'à') for elem in x])
rdd2 = rdd1.map(lambda x: [elem.replace('\xa0', '') for elem in x])
rdd3 = rdd2.map(lambda x: [elem.replace('Ã©', 'é') for elem in x])
rdd4 = rdd3.map(lambda x: [elem.replace('€1', '€') for elem in x])
rdd5 = rdd4.map(lambda x: [elem.replace('€', '') for elem in x])
rdd6 = rdd5.map(lambda x: [elem.replace('Ã¨', 'è') for elem in x])

# 2. Replace the missing values with the mean of the column
rdd7 = rdd6.filter(lambda x: x[0] != '')
rdd8 = rdd7.map(lambda x: (int(x[0]), x[1], intoInt(x[2]),intoInt(x[3]),intoInt(x[4])))
rddmean = rdd8.map(lambda x: x[2]).filter(lambda x: x != '-')
mean = rddmean.mean()
res = rdd8.map(lambda x: (x[0], x[1], valToMean(x[2], mean), x[3], x[4]))

# 3. Save the result in multiple files
res.saveAsTextFile('output/TransformationCO2')

