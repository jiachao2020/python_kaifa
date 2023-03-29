import findspark

findspark.init()

import pyspark
# from pyspark import SparkConf
# from pyspark import SparkContext
# if __name__ == '__main__':
#     conf=SparkConf()
#     conf=conf.setAppName("wordcount").setMaster("local")
#     sc=SparkContext(conf=conf)
#     lines=sc.textFile(r"D:\games\steak\age-0307.txt",2)
#     print(type(lines))


from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import Row


import os
# 遍历文件夹

SparkSession = SparkSession.builder.config("spark.local.dir", "D:/TmpData").appName('all_phone').master("local[*]").enableHiveSupport().getOrCreate()
for root, dirs, files in os.walk(r"E:\220914\220914"):
    # root 表示当前访问的文件夹路径
    # dirs 表示该文件夹下的子目录名list
    # files 表示该文件夹下的文件list
    # 遍历文件
    for f in files:
        #print(os.path.join(root, f))
        lines = SparkSession.read.csv(os.path.join(root, f))
        lines.createOrReplaceTempView("MID_table")
        SparkSession.table("MID_table").cache()
        SparkSession.sql("insert into chaojia.XJH_220914  select _c0 as phone,'{}' as tag from MID_table".format(f))

# SparkSession  = SparkSession.builder.config("spark.local.dir","D:/TmpData").appName('all_phone').master("local[*]").enableHiveSupport().getOrCreate()
#
# lines=SparkSession.read.csv(r"D:\games\steak\age-0307.txt")
# lines.show()
