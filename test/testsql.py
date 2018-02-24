from Database.RedisSql import RedisSql
from Database.MongoSql import MongoSql


def testMongo():
    sql = MongoSql()
    sql.init_db()
    # 测试插入
    proxy = {'ip': '192.168.1.1', 'port': int('80'), 'types': 0, 'protocol': 0, 'country': u'中国', 'area': u'四川',
             'speed': 0}
    sql.insert(proxy)
    proxy = {'ip': '192.168.1.1', 'port': 80, 'types': 0, 'protocol': 0, 'country': '中国', 'area': '广州', 'speed': 11.123}
    sql.insert(proxy)
    proxy = {'ip': '192.168.1.1', 'port': 88, 'types': 0, 'protocol': 0, 'country': '中国', 'area': '广州', 'speed': 11.123}
    sql.insert(proxy)
    # 测试查询
    sql.update({'ip': '192.168.1.1', 'port': 80}, {'score': 11})
    print(sql.delete({'area': '广州', 'port': 88}))
    print(sql.update({'port': '88'}, {'score': 233}))
    print(sql.select(conditions={'ip': '192.168.1.1'}))


def test_redis():
    sqlhelper = RedisSql()
    sqlhelper.init_db('redis://user:wenboyu@118.89.150.14:6379/0')
    proxy = {'ip': '192.168.1.1', 'port': 80, 'type': 0, 'protocol': 0, 'country': '中国', 'area': '广州', 'speed': 11.123,
             'types': 1}
    proxy2 = {'ip': 'localhost', 'port': 433, 'type': 1, 'protocol': 1, 'country': u'中国', 'area': u'广州', 'speed': 123,
              'types': 0, 'score': 100}
    # assert 断言，打印出错误的代码
    assert sqlhelper.insert(proxy) == True
    assert sqlhelper.insert(proxy2) == True
    # assert sqlhelper.get_keys({'types': 1}) == ['proxy::192.168.1.1:80:0']
    # assert sqlhelper.select(conditions={'protocol': 0}) == [('192.168.1.1', '80', '10')]
    # assert sqlhelper.update({'types': 1}, {'score': 888}) == 1
    # assert sqlhelper.select() == [('192.168.1.1', '80', '888'), ('localhost', '433', '100')]
    # assert sqlhelper.delete({'types': 1}) == 1
    # # sqlhelper.drop_db()
    # print('All pass.')


if __name__ == '__main__':
    test_redis()
