from __future__ import unicode_literals

from redis import Redis

import config
from Database.ISQL import Isql
from Database.Sql import Proxy


class RedisSql(Isql):
    def __init__(self, url=None):
        self.index_names = ('types', 'protocol', 'country', 'area', 'score')
        self.redis_url = url or config.DB_CONFIG['DB_CONNECT_STRING']

    def get_proxy_name(self, ip=None, port=None, protocal=None, proxy=None):
        '''
        return: proxy::ip:port:protocal
        '''
        ip = ip or proxy.ip
        port = port or proxy.port
        protocal = protocal or proxy.protocol
        return "proxy::{}:{}:{}".format(ip, port, protocal)

    def get_index_name(self, index_name, value=None):
        '''
        index_name:索引名
        value:索引的value
        return: index::index_name:value
        '''
        if index_name == 'score':
            return 'index::score'
        return "index::{}:{}".format(index_name, value)

    def get_proxy_by_name(self, name):
        '''
        name:文档名
        return:文档所有数据
        '''
        # 通过获得这条数据所有的属性和值，h--hash
        pd = self.redis.hgetall(name)
        if pd:
            # k,v编码为utf8
            return Proxy(**{k.decode('utf8'): v.decode('utf8') for k, v in pd.items()})

    def init_db(self, url=None):
        self.redis = Redis.from_url(url or self.redis_url)

    def drop_db(self):
        return self.redis.flushdb()

    # 通过索引获得数据
    def get_keys(self, conditions):
        '''
        conditions: 查询表的字典
        return: 表名 proxy::{ip}:{port}:{protocol}
        '''
        # 如果ip和port存在，返回proxy::{ip}:{port}:{*}
        if 'ip' in conditions and 'port' in conditions:
            return self.redis.keys(self.get_proxy_name(conditions['ip'], conditions['port'], '*'))

        # select_keys = index::{key}:{value}
        select_keys = {self.get_index_name(key, conditions[key])
                       for key in conditions.keys()
                       if key in self.index_names}
        # sinter 返回交集,并编码
        if select_keys:
            return [name.decode('utf8') for name in self.redis.sinter(keys=select_keys)]
        return []

    def insert(self, value):
        '''
        value:传入的字典数据
        return:插入的数量

        1.得到Proxy类结构，并赋值给mapping
        2.检查mapping的key
        3.通过get_proxy_name获得代理名，并存储数据
        4.创建索引
        '''
        # 1.
        proxy = Proxy(ip=value['ip'], port=value['port'], types=value['types'],
                      protocol=value['protocol'], country=value['country'], area=value['area'],
                      speed=value['speed'], score=value.get('score', config.DEFAULT_SCORE))
        # 返回proxy.__dict__的内容为字典，属性名key，属性值为value，及参数value的内容
        mapping = proxy.__dict__
        # 2.
        for k in list(mapping.keys()):
            # 检查属性（字符串）是否以 '_'开头, pop(k)弹出
            if k.startswith('_'):
                mapping.pop(k)
            # 3.
            # 得到代理名为proxy::{ip}:{port}:{protocal}
            object_name = self.get_proxy_name(proxy=proxy)
            # 存储数据，返回数量，使用hash set存储
            insert_num = self.redis.hmset(object_name, mapping)

            # 4. 创建索引，即循环得到index_name，object为代理名，传入proxy得到索引的值
            if insert_num > 0:
                for index_name in self.index_names:  # index_names = ('types', 'protocol', 'country', 'area', 'score')
                    self.create_index(index_name, object_name, proxy)
            return insert_num

    # 创建索引
    def create_index(self, index_name, object_name, proxy):
        """
        index_name: 索引列表名
        object_name:表名
        proxy:表数据
        return:1成功，0失败
        """
        # getattr(proxy, index_name) 获得是proxy中index_name 对应的值value,通过get_index_name拼凑语句：
        # 例 redis_key = index::area:广州
        redis_key = self.get_index_name(index_name, getattr(proxy,
        index_name))  # getattr 获取对象object的属性或者方法，如果存在打印出来，如果不存在，打印出默认值，默认值可选
        # 如果名字是score 则添加为（表=redis_key,值=object_name,score=proxy.score）
        if index_name == 'score':
            return self.redis.zadd(redis_key, object_name, int(proxy.score))
        # 添加集合(表=redis_key,值=obeject_name)
        return self.redis.sadd(redis_key, object_name)

    def delete(self, conditions):
        """
        conditions: 查询表的字典
        return:成功次数
        1.删除索引
        2.删除数据
        """
        # 1.
        # proxy_keys=表名 ，index_keys=index::*数据
        proxy_keys = self.get_keys(conditions)
        index_keys = self.redis.keys(u"index::*")
        if not proxy_keys:
            return 0
        # 循环所有index内容，删除index::score下的proxy_keys
        #       删除其他索引下的proxy_keys
        for iname in index_keys:
            if iname == b'index::score':
                self.redis.zrem(self.get_index_name('score'), *proxy_keys)
            else:
                self.redis.srem(iname, *proxy_keys)
        #2.
        return self.redis.delete(*proxy_keys) if proxy_keys else 0

    def update(self, conditions, values):
        '''
        conditions:查询表的字典
        values: 更新的数据
        return: count：返回更新成功次数
        1.通过conditions拿到 表名
        2.循环每个表名，循环value的k,v
            如果k==score删删除index::score对应的内容，再添加
            使用value的k,v更新name内容
        '''
        # 1.
        objects = self.get_keys(conditions)
        count = 0
        # 2.
        for name in objects:
            for k, v in values.items():
                if k == 'score':
                    #  Zrem 命令用于移除有序集中的一个或多个成员，不存在的成员将被忽略。
                    # 根据索引。删除name的数据
                    # 重新根据索引添加，name和value，达到更新score的目的
                    self.redis.zrem(self.get_index_name('score'), [name])
                    self.redis.zadd(self.get_index_name('score'), name, int(v))
                # 更新内容
                self.redis.hset(name, key=k, value=v)
                count += 1
            return count

    def select(self, count=None, conditions=None):
        """
        conditions：查表的字典
        return: 查询结果
        1.限制最大查询量
        2.查询条件存在index，querys=返回字典类型
        如果querys存在
            3.通过querys得到数据，并通过score排序
        不存在
            4.通过分数查询得到数据
        5.通过数据查询到proxy全部信息get_proxy_by_name，用元组格式存储在list当中

        """
        # 1.最多返回1000条数据
        count = (count and int(count)) or 1000
        count = 1000 if count > 1000 else count  # 如果count>1000，count=1000, 否则count还是count

        # 链表表达式在for语句前面，for后面就是对参数的限定，及最后执行 k: v
        # 如果conditions存在，执行{ }
        # 2.如果k在self.index_names中，执行for循环，k:v为字典
        querys = {k: v for k, v in conditions.items() if k in self.index_names} if conditions else None
        # 3.
        if querys:
            objects = list(self.get_keys(querys))[:count]  # 数组化 get_keys(querys)得到查找的表名[0：count]
            redis_name = self.get_index_name('score')  # redis_name = index::score
            # zscore 得到score分数。sort从小到大排序
            objects.sort(key=lambda x: int(self.redis.zscore(redis_name, x)))
        else:
            # 4.
            objects = list(
                # ZREVRANGEBYSCORE 返回有序集合中指定分数区间内的成员，分数由高到低排序。
                # "+inf" 或者 "-inf" 来表示记录中最大值和最小值。
                self.redis.zrevrangebyscore(self.get_index_name("score"), '+inf', '-inf', start=0, num=count)
            )
        # 5.
        result = []
        for name in objects:
            p = self.get_proxy_by_name(name)
            result.append((p.ip, p.port, p.score))
        return result
