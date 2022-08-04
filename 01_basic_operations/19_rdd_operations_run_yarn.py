from codecs import ascii_encode
from pyspark import SparkConf, SparkContext
import json
from defs_19 import city_with_category

if __name__ == "__main__":
    conf = SparkConf().setAppName("test-yarn-1").setMaster("yarn")
    conf.set("spark.submit.pyFiles", "/home/dev/Code/example/defs_19.py")
    sc = SparkContext(conf = conf)
    
    file_rdd = sc.textFile("hdfs://master:9000/inputs/order.text")
    
    jsons_rdd = file_rdd.flatMap(lambda line: line.split("|"))
    
    dict_rdd = jsons_rdd.map(lambda x: json.loads(x))
    
    beijing_rdd = dict_rdd.filter(lambda x: x['areaName'] == '北京')
    category_rdd = beijing_rdd.map(city_with_category)
    
    result_rdd = category_rdd.distinct()
    
    print(result_rdd.collect())