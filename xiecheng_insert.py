# coding=utf-8
from gevent import monkey;monkey.patch_all()
import gevent
import requests
import time
import pymysql


def data_handler(anum,num):
    conn = pymysql.connect(host='172.18.3.204',user='root',password='xinwei',database='btree',charset='utf8')
    cursor = conn.cursor()
    for i in range(anum,num):
        sql = 'insert into aaa(sid,name,email) values(%s,%s,concat(%s,"hael","@163"));'
        res = cursor.execute(sql,[i,"root",i])
        conn.commit()
    cursor.close()
    conn.close()

start_time=time.time()

gevent.joinall([
    gevent.spawn(data_handler,1,2000),
    gevent.spawn(data_handler,2001,5000),
    gevent.spawn(data_handler,5001,8000),
    gevent.spawn(data_handler,8001,10000),
])


stop_time=time.time()
print('run time is %s' %(stop_time-start_time))