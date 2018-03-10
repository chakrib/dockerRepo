from pyspark import SparkContext, SQLContext

sc = SparkContext(appName = 'emrtest')
sqlCtx = SQLContext(sc)

print sc

rdd = sc.parallelize(['1','2','3'])


print 'the count in rdd is ',rdd.count()

rdd.repartition(1).saveAsTextFile('hdfs://ec2-52-87-197-156.compute-1.amazonaws.com/tmp/count')
