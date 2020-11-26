from pyspark.sql import SparkSession
from collections import Counter

spark = SparkSession \
    .builder \
    .appName("traceroute_analysis") \
    .getOrCreate()
spark_context = spark.sparkContext

data_list = [((u'192.168.1.1', None), 239763), ((u'2001:14f0::218', u'12355'), 196613),
             ((u'2001:14f0::204', u'12355'), 196330), ((u'2001:14f0:3::41', u'12355'), 171057),
             ((u'2001:14f0::221', u'12355'), 139893), ((u'192.168.0.1', None), 110226),
             ((u'2001:470:0:2cf::1', u'6939'), 107359), ((u'2001:470:0:227::2', u'6939'), 83704),
             ((u'2001:470:0:3ea::2', u'6939'), 66815), ((u'2001:470:0:410::2', u'6939'), 64259),
             ((u'2001:470:0:489::2', u'6939'), 63966), ((u'2001:470:0:440::1', u'6939'), 63526),
             ((u'2001:14f0::214', u'12355'), 60094), ((u'192.168.1.254', None), 54788),
             ((u'192.168.178.1', None), 54770), ((u'2001:7f8::1b1b:0:1', None), 54291),
             ((u'2001:470:0:277::1', u'6939'), 52707), ((u'95.59.172.18', u'9198'), 51216),
             ((u'2001:470:0:404::1', u'6939'), 49282), ((u'80.241.3.66', u'21282'), 47457)]

d = Counter(data_list)

res = [k for k, v in d.items() if v > 1]
print res
list_d = list(d)
counter_rdd = spark_context.parallelize(list_d)
ip_asn_counter_rdd = counter_rdd.map(lambda x: (x[0][0], x[0][1], x[1]))
counter_df = ip_asn_counter_rdd.toDF(schema=['ip', 'asn', 'counter'])
counter_df.coalesce(numPartitions=1).write.csv(path='/home/selim/data/counter_result', header=True)
