from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel

import subnetwork_util
from pyspark.sql.functions import concat_ws
from operator import add

file_name = '/home/selim/data/traceroute-1000lines.txt'
file_hour_zero = 'file:///home/selim/data/traceroute-2020-10-20T0000'

as_ipv4 = '/home/selim/data/routeviews-rv2-20201020-0200.pfx2as'
as_ipv6 = '/home/selim/data/routeviews-rv6-20201020-1200.pfx2as'

spark = SparkSession \
    .builder \
    .appName("traceroute_analysis") \
    .getOrCreate()

spark_context = spark.sparkContext
spark_context.setCheckpointDir('/home/selim/data/checkPointDir/')

traceroute_data = spark.read.json(path=file_name)
result_column = traceroute_data.select('result')
result_rdd = result_column.rdd
hops_list = result_rdd.flatMap(lambda v: v['result'])
hops = hops_list.flatMap(lambda row: row).filter(lambda row: type(row) is list)


def deduplication(hops_list_item):
    deduplication_set = set()
    for item in hops_list_item:
        deduplication_set.add(item['from'])
    return deduplication_set


ip_list = hops.flatMap(deduplication).filter(lambda element: element is not None)


def ip_to_integer(ip):
    ip_integer, version = subnetwork_util.ip_to_integer(ip)
    return ip, ip_integer, version


ip_integer_version_dataframe = ip_list.map(lambda ip: ip_to_integer(ip))
ip_integer_version_cache = ip_integer_version_dataframe.cache()
ip_integer_version_cache.checkpoint()

as_ip_dataframe = spark.read.csv(path=[as_ipv4, as_ipv6], sep='\t').toDF('ip_addr', 'prefix', 'asn')
asn_df = as_ip_dataframe.withColumn('subnet', concat_ws('/', as_ip_dataframe.ip_addr, as_ip_dataframe.prefix))


def lower_upper_network_util(row):
    ip_lower, ip_upper, version = subnetwork_util.subnetwork_to_ip_range(row.subnet)
    return row.subnet, row.asn, ip_lower, ip_upper, version


subnet_asn_ranged_df = asn_df.rdd.map(lambda row: lower_upper_network_util(row))


def key_value_splitter(row):
    subnet, asn, ip_lower, ip_upper, version = row
    if version == '4' or version == 4:
        first_octet = str(subnet).split('.')[0]
    else:
        first_octet = str(subnet).split(':')[0]
    return first_octet, [row]


kv_subnet_asn_df = subnet_asn_ranged_df.map(key_value_splitter)
reduced_kv_subnet_asn_df = kv_subnet_asn_df.reduceByKey(add)
reduced_kv_subnet_asn_df_cached = reduced_kv_subnet_asn_df.cache()
reduced_kv_subnet_asn_df_cached.checkpoint()
asn_dictionary = reduced_kv_subnet_asn_df.collectAsMap()
broadcast_asn_dictionary = spark_context.broadcast(asn_dictionary)


def find_asn(ip, ip_integer, version1):
    if version1 == '4' or version1 == 4:
        first_octet = str(ip).split('.')[0]
    else:
        first_octet = str(ip).split(':')[0]

    asn_dictionary_value = broadcast_asn_dictionary.value
    if first_octet in asn_dictionary_value:
        search_list = asn_dictionary_value[first_octet]
        for subnet, asn, ip_lower, ip_upper, version2 in search_list:
            if version1 == version2:
                if ip_lower <= ip_integer <= ip_upper:
                    return str(ip), str(asn)
        return str(ip), None
    else:
        return str(ip), None


ip_asn = ip_integer_version_cache.map(lambda (ip, ip_integer, version): find_asn(ip, ip_integer, version))
cached_ip_asn = ip_asn.cache()
cached_ip_asn = ip_asn.persist(StorageLevel.DISK_ONLY)
cached_ip_asn.checkpoint()
cached_ip_asn.coalesce(numPartitions=1, shuffle=False).saveAsTextFile('/traceroute/data/results')
print ip_asn

ip_asn.sample(withReplacement=False, fraction=0.1)
ip_asn.toDF().write.csv(path='/traceroute/data/results/ipAsnDf.csv', sep=',', header=True)
ip_asn.toDF().repartition(1)

