# pip install findspark
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

from pyspark.sql import Row

SparkSession  = SparkSession.builder.appName('all_phone').master("local[*]").enableHiveSupport().getOrCreate()
lines=SparkSession.read.csv(r"D:\games\steak\age-0307.txt")
#a=SparkSession.sql("select * from chaojia.phone_all")
#print(a.show())
#print(lines.show())
haoduan=['185', '136', '198', '131', '147', '148', '153', '146', '133', '187', '170', '157', '195', '132', '178', '156', '199', '162', '134', '130', '145', '165', '183', '172', '137', '175', '184', '191', '189', '188', '139', '180', '193', '182', '177', '166', '138', '158', '159', '149', '173', '176', '196', '174', '135', '186', '152', '150', '151', '171', '167', '155', '181']
phone_last8=SparkSession.range(0,100000000)
phone_last8.registerTempTable("all_last_8_tmp")
for i in haoduan:
    a=SparkSession.sql("insert overwrite table chaojia.all_phone partition  (haoduan='{}') select concat({},lpad(id,8,0)) as phone,md5(concat({},lpad(id,8,0))) as phonemd5 from all_last_8_tmp".format(i,i,i))