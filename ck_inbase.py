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

#.此方法可以对列更名
SparkSession.sql("set hive.exec.dynamic.partition=true")
SparkSession.sql("set hive.exec.dynamic.partition.mode=nonstrict")
SparkSession.sql('''insert overwrite table chaojia.CK_PRODUCT_MONTH_WAY_phone
partition(PRODUCT,month,way)
select b.phone,a.PRODUCT,a.month,a.way from chaojia.CK_PRODUCT_MONTH_WAY_1 a left join chaojia.all_phone b on a.phonemd5=b.phonemd5 where b.phone is not null and a.product rlike 'ZAXD' ''')