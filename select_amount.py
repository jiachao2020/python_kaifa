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

SparkSession  = SparkSession.builder.config("spark.local.dir","D:\TmpData").appName('CK-IN').master("local[*]").enableHiveSupport().getOrCreate()
lines=SparkSession.read.csv(r"D:\games\steak\XJH-AGE-NEW").withColumnRenamed('_c0','phonemd5').withColumn("xh",monotonically_increasing_id())
#.此方法可以对列更名
lines.createOrReplaceTempView("CK")
lines_turn11=SparkSession.sql('insert overwrite table chaojia.ck_2302_md5 select row_number() over (order by xh),phonemd5 from CK')