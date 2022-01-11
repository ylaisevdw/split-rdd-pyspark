from pyspark.sql import Row
from pyspark import SparkContext
from pyspark.sql import SparkSession

# Init some test data
sc = SparkContext(master="local[*]", appName="inclusion_detector")
spark = SparkSession.builder.appName("inclusion_detector").getOrCreate()
NR_OF_SPLITS = 5
row = Row("value")
rdd1 = sc.parallelize(range(100))
df1 = rdd1.map(row).toDF()
rdd1_repartitioned = df1.repartition(NR_OF_SPLITS, "value").rdd
rdd2 = sc.parallelize(range(300))
df2 = rdd2.map(row).toDF()
rdd2_repartitioned = df2.repartition(NR_OF_SPLITS, "value").rdd


def f(id, iterator):
    samples = [[] for _ in range(NR_OF_SPLITS)]
    for row in iterator:
        samples[id - 1].append(row)
    yield samples


def flat_mux_partitions_with_index(rdd):
    mux = rdd.mapPartitionsWithIndex(f)
    return [mux.mapPartitions(lambda it: list(next(it))[i]) for i in range(NR_OF_SPLITS)]


def split_rdd(rdd):
    return flat_mux_partitions_with_index(rdd)


splitted_rdd = split_rdd(rdd1_repartitioned)
# However, splitted_rdd[0].collect() is same as splitted_rdd[i].collect() for i in range(1,5)
# i is fixed in the array, it does not make use of the for loop