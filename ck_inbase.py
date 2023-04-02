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

SparkSession  = SparkSession.builder.config("spark.local.dir").appName('all_phone').master("local[*]").config("spark.sql.shuffle.partitions", "30").enableHiveSupport().getOrCreate()
conf=SparkSession.sparkContext._conf.setAll([('spark.executor.memory', '8g'),
('spark.app.name', 'Spark Updated Conf'),
('spark.driver.cores', '1'), ('spark.executor.cores', '15'),
('spark.driver.memory','2g')])
#lines=SparkSession.read.csv(r"E:\220914\steak\age-0307.txt")
#lines.toDF("phonemd5",'age').createOrReplaceTempView('xjh_age_0307')
#SparkSession.sql('''create table age.xjh_age_0307 as select phonemd5,age from xjh_age_0307''')
haoduan=['130','185', '136', '198', '131', '147', '148', '153', '146', '133', '187', '170', '157', '195', '132', '178', '156', '199', '162', '134', '145', '165', '183', '172', '137', '175', '184', '191', '189', '188', '139', '180', '193', '182', '177', '166', '138', '158', '159', '149', '173', '176', '196', '174', '135', '186', '152', '150', '151', '171', '167', '155', '181']
SparkSession.sql("set hive.exec.dynamic.partition=true")
SparkSession.sql("set hive.exec.dynamic.partition.mode=nonstrict")
#.此方法可以对列更名
for i in haoduan:
    SparkSession.sql('''insert overwrite table chaojia.CK_PRODUCT_MONTH_WAY_phone
partition(PRODUCT,month,way,haoduan)
select b.phone,a.PRODUCT,a.month,a.way,b.haoduan from chaojia.CK_PRODUCT_MONTH_WAY_1 a left join chaojia.all_phone b on a.phonemd5=b.phonemd5 where b.phone is not null and b.haoduan='{}' '''.format(i))