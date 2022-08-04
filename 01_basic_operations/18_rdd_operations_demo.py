from codecs import ascii_encode
from pyspark import SparkConf, SparkContext
import json

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    
    file_rdd = sc.textFile("file:///home/dev/data/order.text")
    
    jsons_rdd = file_rdd.flatMap(lambda line: line.split("|"))
    
    dict_rdd = jsons_rdd.map(lambda x: json.loads(x))
    
    beijing_rdd = dict_rdd.filter(lambda x: x['areaName'] == '北京')
    category_rdd = beijing_rdd.map(lambda x: (x["areaName"]+ "_" + x["category"]))
    
    result_rdd = category_rdd.distinct()
    
    print(result_rdd.collect())