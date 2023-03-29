# coding=utf-8
import datetime
import hashlib
import threading

import pymysql


def md5_process(a):
    md5 = hashlib.md5()  # md5对象，md5不能反解，但是加密是固定的
    # update需要一个bytes格式参数
    md5.update(str.encode(a.strip()))  # 要对哪个字符串进行加密，就放这里
    value = md5.hexdigest()
    return value  # 拿到加密字符串

haoduan=['185', '136', '198', '131', '147', '148', '153', '146', '133', '187', '170', '157', '195', '132', '178', '156', '199', '162', '134', '130', '145', '165', '183', '172', '137', '175', '184', '191', '189', '188', '139', '180', '193', '182', '177', '166', '138', '158', '159', '149', '173', '176', '196', '174', '135', '186', '152', '150', '151', '171', '167', '155', '181']


'''1. 只有线程操作数据'''


def onlyHaveThreading():
    def insert_DB():
        conn = pymysql.connect(host='xx.xx.xxx.xxx', user='root', password="123456",
                               database='xiancheng', port=3306,
                               charset='utf8')
        cus = conn.cursor()
        try:
            for i in range(1000):
                now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                create_tm = datetime.datetime.strptime(now_str, '%Y-%m-%d %H:%M:%S')
                sql = ("INSERT INTO test_table VALUES ('%s','%s','%s')") % (i, i, create_tm)
                # tlock.acquire()
                ok = cus.execute(sql)
                conn.commit()
                # print(i)
                # tlock.release()
        except Exception as e:
            print("one error happen", e)
        finally:
            cus.close()
            conn.close()

    class myThread(threading.Thread):
        def __init__(self, id):
            threading.Thread.__init__(self)
            self.id = id
            pass

        def run(self):
            insert_DB()
            # print ("开始操作%s"%i)

    threads = []
    tlock = threading.Lock()
    for i in range(10):
        thread = myThread(i)
        threads.append(thread)

    for i in range(len(threads)):
        threads[i].start()


# 计算程序的运行的时间
def timeCostNum(start_time):
    end_time = datetime.datetime.now()
    spend_time = (end_time - start_time)
    print(spend_time)
    start_time = start_time.strftime("%Y-%m-%d %H:%M:%S")
    end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")
    minutes = spend_time / 60
    second = spend_time // 60
    timeStr = str(minutes) + '分钟' + str(second) + "秒"
    print(timeStr)


'''2. 线程操作数据+ 不同的数据'''


def DifferentDataOnThreading():
    def test_sql_insert(start, end):
        conn = pymysql.connect(host='localhost',
                     user='root',
                     password='386561',
                     database='chaojia',
                     charset='utf8')
        cus = conn.cursor()
        try:
            values = []
            for i in range(start, end):
                first3='188'
                last8 = str(i).zfill(8)
                phone = first3 + last8
                # print(phone)

                phone_md5 = md5_process(phone)
                values.append("('" + phone + "','" + phone_md5 + "')")
                value = ','.join(values)

            sql = '''insert into ALL_phone_md5
                        value {}
                        '''.format(value)
            values=[]
            cus.execute(sql)
            conn.commit()
            # print(i)
            # tlock.release()
        except Exception as e:
            print("one error happen", e)
        finally:
            cus.close()
            conn.close()

    def insert_DB2(id):
        if id == 1:
            print(f'开启任===务- ={1}')
            # for i in range(200):
            #     print('111','== ',i)
            start = 0
            end = 100
            test_sql_insert(start, end)
        elif id == 2:
            print(f'开启任===务- ={2}')
            # for i in range(200):
            #     print('222','== ',i)
            start = 100
            end = 200
            test_sql_insert(start, end)
        elif id == 3:
            print(f'开启任===务- ={3}')
            # for i in range(200):
            #     print('333','== ',i)
            start = 200
            end = 300
            test_sql_insert(start, end)
        elif id == 4:
            print(f'开启任===务- ={4}')
            # for i in range(200):
            #     print('444','== ',i)
            start = 300
            end = 400
            test_sql_insert(start, end)
        elif id == 5:
            print(f'开启任===务- ={5}')
            # for i in range(200):
            #     print('555','== ',i)
            start = 400
            end = 500
            test_sql_insert(start, end)
        elif id == 6:
            print(f'开启任===务- ={6}')
            # for i in range(200):
            #     print('555','== ',i)
            start = 500
            end = 600
            test_sql_insert(start, end)
        elif id == 7:
            print(f'开启任===务- ={7}')
            # for i in range(200):
            #     print('555','== ',i)
            start = 600
            end = 700
            test_sql_insert(start, end)
        elif id == 8:
            print(f'开启任===务- ={8}')
            # for i in range(200):
            #     print('555','== ',i)
            start = 700
            end = 800
            test_sql_insert(start, end)

        elif id == 9:
            print(f'开启任===务- ={9}')
            # for i in range(200):
            #     print('555','== ',i)
            start = 800
            end = 900
            test_sql_insert(start, end)

        elif id == 0:
            print(f'开启任===务- ={10}')
            # for i in range(200):
            #     print('555','== ',i)
            start = 900
            end = 1000
            test_sql_insert(start, end)

    class myThread(threading.Thread):
        def __init__(self, id):
            threading.Thread.__init__(self)
            self.id = id
            pass

        def run(self):
            # insert_DB()
            # print(id)
            insert_DB2(i)
            print("开始操作%s" % i)

    threads = []
    tlock = threading.Lock()
    for i in range(10):
        thread = myThread(i)
        threads.append(thread)

    for i in range(len(threads)):
        threads[i].start()


if __name__ == '__main__':
    '''1. 只有线程操作数据,多任务'''
    start_time = datetime.datetime.now()
    # onlyHaveThreading()
    # timeCostNum(start_time)
    '''insert_DB() 不传入id，只是重复插入数据啊，需要改造'''

    ''''''
    '''2. 线程操作数据+ 不同的数据 打印操作-debug模式'''
    DifferentDataOnThreading()
    timeCostNum(start_time)

    '''
    1000
no   0:00:45.412410
yes  0:00:00.002991
    '''
