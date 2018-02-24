import sys
from config import DB_CONFIG

from util.exception import Con_DB_Fail

# 根据配置文件里面连接类型，进入相应数据库操作
try:
    if DB_CONFIG['DB_CONNECT_TYPE'] == 'pymongo':
        from Database.MongoSql import MongoSql as Sql
    elif DB_CONFIG['DB_CONNECT_TYPE'] == 'redis':
        from Database.RedisSql import RedisSql as Sql
    else:
        from Database.Sql import Sql as Sql
    sql = Sql()
    sql.init_db()
except Exception as e:
    raise Con_DB_Fail


# 读取队列中的数据，写入数据库中
def store_date(queue, db_proxy_num):
    successNum = 0
    failNum = 0
    while True:
        try:
            proxy = queue.get(timeout=300)
            if proxy:
                sql.insert(proxy)
                successNum += 1
            else:
                failNum += 1
            str = 'ProxyPool----->>>>>>>>Success ip num :%d, Fail ip num:%d' % (successNum, failNum)
            sys.stdout.write(str + "\r")
            sys.stdout.flush()
        except BaseException as e:
            if db_proxy_num.value != 0:
                successNum += db_proxy_num.value
                db_proxy_num.value = 0
                str = 'ProxyPool----->>>>>>>>Success ip num :%d, Fail ip num:%d' % (successNum, failNum)
                # 字符写到标准输出中，print是对sys.stdout.write有好封装
                # print 可以把一个对象转化成str然后放到标准输出中， sys.stdout.write 需要把对象先转化成对象在输出
                sys.stdout.write(str + "\r")
                sys.stdout.flush()  # 刷新stdout
                successNum = 0
                failNum = 0
