from pyspark.sql import SparkSession
import subnetwork_util
from pyspark.sql.functions import concat_ws
import multiprocessing
from joblib import Parallel, delayed

num_cores = multiprocessing.cpu_count() - 1

file_name = '/home/selim/data/traceroute-1000lines.txt'
file_hour_zero = 'file:///home/selim/data/traceroute-2020-10-20T0000'

as_ipv4 = '/home/selim/data/routeviews-rv2-20201020-0200.pfx2as'
as_ipv6 = '/home/selim/data/routeviews-rv6-20201020-1200.pfx2as'

spark = SparkSession \
    .builder \
    .appName("traceroute_analysis") \
    .getOrCreate()

spark_context = spark.sparkContext

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

as_ip_dataframe = spark.read.csv(path=[as_ipv4, as_ipv6], sep='\t').toDF('ip_addr', 'prefix', 'asn')
asn_df = as_ip_dataframe.withColumn('subnet', concat_ws('/', as_ip_dataframe.ip_addr, as_ip_dataframe.prefix))


def lower_upper_network_util(row, subnet):
    ip_lower, ip_upper, version = subnetwork_util.subnetwork_to_ip_range(subnet)
    return subnet, row.asn, ip_lower, ip_upper, version


subnet_asn_ranged_df = asn_df.rdd.map(lambda row: lower_upper_network_util(row, row.subnet))
subnet_asn_ranged_list = subnet_asn_ranged_df.collect()

subnet_asn = spark_context.broadcast(subnet_asn_ranged_list)


def match_function(subnet, asn, ip_lower, ip_upper, version2, ip, ip_integer, version1):
    if version1 == version2 and ip_lower <= ip_integer <= ip_upper:
        return ip, asn
    return ip, None


def find_asn(ip, ip_integer, version1):
    subnet_asn_list = subnet_asn.value
    processed_list = Parallel(n_jobs=num_cores)(delayed(match_function)(subnet, asn, ip_lower, ip_upper, version2, ip, ip_integer, version1) for subnet, asn, ip_lower, ip_upper, version2 in subnet_asn_list)
    return processed_list


ip_asn = ip_integer_version_dataframe.map(lambda (ip, ip_integer, version): find_asn(ip, ip_integer, version)).collect()
print ip_asn

""" untouched function


Parallel(n_jobs=num_cores)(delayed(my_function(i,parameters) 
                                                        for i in inputs)



def find_asn(ip, ip_integer, version1):
    for subnet, asn, ip_lower, ip_upper, version2 in subnet_asn.value:
        if version1 == version2 and ip_lower <= ip_integer <= ip_upper:
            return ip, asn
    return ip, None
"""

"""
# for flattening and removing same ip numbers from all the ip_list 
"""
# hops_flattened = hops.flatMap(lambda a: a)
# from_ip_list = hops_flattened.map(lambda b: b['from']).filter(lambda c: c is not None)
# unique_from_ip_list = from_ip_list.distinct()
# print(unique_from_ip_list.collect())

"""
# for counting the duplicates of ip list: https://stackoverflow.com/a/52072456
"""

# matched_ip_network = ipv4_broadcast_list.value.filter(lambda net: ip in net).collect()
# if len(matched_ip_network) == 0:
#     return ip, None
# elif len(matched_ip_network) == 1:
#     return ip, matched_ip_network[0]
# else:
#     return ip, matched_ip_network
