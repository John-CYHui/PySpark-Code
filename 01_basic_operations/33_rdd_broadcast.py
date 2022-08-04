from codecs import ascii_encode
from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    
    stu_info_list = [(1, '张大仙', 11),
                    (2, '王晓晓', 13),
                    (3, '张甜甜', 11),
                    (4, '王大力', 11)]
    
    broadcast = sc.broadcast(stu_info_list)
    
    score_info_rdd = sc.parallelize([
        (1, '语文', 99),
        (2, '数学', 99),
        (3, '英语', 99),
        (4, '编程', 99),
        (1, '语文', 99),
        (2, '编程', 99),
        (3, '语文', 99),
        (4, '英语', 99),
        (1, '语文', 99),
        (3, '英语', 99),
        (2, '编程', 99)
    ])
    
    def map_func(data):
        id = data[0]
        for stu_info in broadcast.value:
            stu_id = stu_info[0]
            if id == stu_id:
                name = stu_info[1]
                return (name, data[1], data[2])  
        
    print(score_info_rdd.map(map_func).collect())