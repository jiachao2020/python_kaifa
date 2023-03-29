# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import pymysql

import hashlib
def md5_process(a):
    md5 = hashlib.md5()   				# md5对象，md5不能反解，但是加密是固定的
# update需要一个bytes格式参数
    md5.update(str.encode(a.strip()))  	# 要对哪个字符串进行加密，就放这里
    value = md5.hexdigest()
    return value# 拿到加密字符串

haoduan=['185', '136', '198', '131', '147', '148', '153', '146', '133', '187', '170', '157', '195', '132', '178', '156', '199', '162', '134', '130', '145', '165', '183', '172', '137', '175', '184', '191', '189', '188', '139', '180', '193', '182', '177', '166', '138', '158', '159', '149', '173', '176', '196', '174', '135', '186', '152', '150', '151', '171', '167', '155', '181']

db = pymysql.connect(host='localhost',
                     user='root',
                     password='386561',
                     database='chaojia',
                     charset='utf8')
 
# 使用 cursor() 方法创建一个游标对象 cursor
cursor = db.cursor()
 
# 使用 execute()  方法执行 SQL 查询 
cursor.execute("SELECT VERSION()")
 
# 使用 fetchone() 方法获取单条数据.
data = cursor.fetchone()
print(data)
 
print ("数据库连接成功！")
'''
# 创建表
sql="""CREATE TABLE ALL_phone_md5 (
          phone  CHAR(11) NOT NULL,
          phone_md5  CHAR(32),
          PRIMARY KEY (`phone`))
          ENGINE=InnoDB DEFAULT CHARSET=utf8;
          """
#运行sql语句
cursor.execute(sql)
'''
values=[]
for first3 in haoduan:
    for i in range(1,100000000):
        last8=str(i).zfill(8)
        phone=first3+last8
        #print(phone)

        phone_md5=md5_process(phone)
        values.append("('"+phone+"','"+phone_md5+"')")
        value = ','.join(values)
    #print(value)

    sql = '''
        insert into ALL_phone_md5
        value {}
        '''.format(value)
    values = []
    #print(sql)
    try:
            # 执行SQL语句
        cursor.execute(sql)
            # 向数据库提交
        db.commit()
    except:
            # 发生错误时回滚
        db.rollback()

# 关闭数据库连接
db.close()
