import datetime

from sqlalchemy import Column, Integer,DateTime, Numeric, create_engine, VARCHAR
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from config import DB_CONFIG, DEFAULT_SCORE

from Database.ISQL import Isql
from util.exception import Con_DB_Fail

'''
sql操作的基类
包括：
ip，端口，types类型(0高匿名，1透明)，protocol(0 http,1 https http),
country(国家),area(省市), updatetime(更新时间),speed(连接速度)
'''

# 声明基类,其子类可以与一个表关联
Base = declarative_base()


# 声明表结构对象
class Proxy(Base):
    __tablename__ = 'proxys'
    id = Column(Integer, primary_key=True, autoincrement=True)  # 主键，不能为空
    ip = Column(VARCHAR(16), nullable=False)
    port = Column(Integer, nullable=False)
    types = Column(Integer, nullable=False)
    protocol = Column(Integer, nullable=False, default=0)
    country = Column(VARCHAR(100), nullable=False)
    area = Column(VARCHAR(100), nullable=False)
    updatetime = Column(DateTime(), default=datetime.datetime.utcnow())
    speed = Column(Numeric(5, 2), nullable=False)
    score = Column(Integer, nullable=False, default=DEFAULT_SCORE)


class Sql(Isql):
    # 得到表结构
    params = {'ip': Proxy.ip, 'port': Proxy.port, 'types': Proxy.types, 'Protocol': Proxy.protocol,
              'country': Proxy.country, 'area': Proxy.area, 'score': Proxy.score}

    def __init__(self):
        # 要连接使用create_engine(),转入 连接字符
        self.engine = create_engine(DB_CONFIG['DB_CONNECT_STRING'], echo=False)
        print("数据库连接信息：" + DB_CONFIG['DB_CONNECT_STRING'])
        #  ORM处理数据库的方式是通过Session来实现的，输入参数绑定到之前的engine上
        DB_Session = sessionmaker(bind=self.engine)
        self.session = DB_Session()

    # 创建表结构
    def init_db(self):
        Base.metadata.create_all(self.engine)

    # 删除表结构
    def drop_db(self):
        Base.metadate.drop_all(self.engine)

    def insert(self, value):
        proxy = Proxy(ip=value['ip'], port=value['port'], types=value['types'], protocol=value['protocol'],
                      country=value['country'], area=value['area'], speed=value['speed'])
        self.session.add(proxy)
        self.session.commit()

    def delete(self, conditions=None):

        '''
        步骤：
        1.得到需要删除的字段
        2.删除字段内容
        '''
        # 1.得到需要删除的字段
        # 构造查询字段
        if conditions:
            conditions_list = []
            for key in list(conditions.keys()):
                # get(key[, default])
                # 如果key在字典里,返回key的值,否则返回default值。
                # 如果default未给出它默认为 None，此方法永远不会引发KeyError。
                if self.params.get(key, None):
                    # print('self.params.get(key)类型为{}，数值为{}'.format(type(self.params.get(key)),self.params.get(key)))
                    # print('conditions.get(key)类型为{}，数值为{}'.format(type(conditions.get(key)),conditions.get(key)))
                    # 构造查询语句Proxy.xx == 需要删除的数据
                    # 因为params的value是sqlalchemy包的内容，因此 == 后构成了查询语句，而不是true，false
                    # 例：意为：proxys.area = 广州, 值为：proxys.area = :area_1
                    conditions_list.append(self.params.get(key) == conditions.get(key))

            conditions = conditions_list

            # query查询，查询Proxy表
            query = self.session.query(Proxy)
            if query:
                for condition in conditions:
                    # 使用构造语句，得到删除字段
                    # 对请求内容过滤，过滤器filter，使用的参数是关键字—>上面构造出来的语句
                    query = query.filter(condition)
                # 2.删除查询内容
                deleteNum = query.delete()  # 使用query.delete差距过滤后的内容，返回行数
                self.session.commit()
        else:
            deleteNum = 0
        return ('deleteNum', deleteNum)

    def update(self, conditions=None, value=None):

        '''
        步骤：
        1.得到需要更新的字段
        2.更改字段内容
        :参数 conditions,是个字典。类似self.params
        :参数 Value:也是一个字典
        '''
        # 1.得到需要更新的字段
        # 构造查询条件语句，得到需要更新的字段
        if conditions and value:
            condition_list = []
            for key in list(conditions.keys()):
                if self.params.get(key, None):
                    condition_list.append(self.params.get(key) == conditions.get(key))
            conditions = condition_list
            query = self.session.query(Proxy)
            for condition in conditions:
                # 得到需要更新字段
                query = query.filter(condition)

            # 2.更改字段内容
            # 构造更新语句
            updatevalue = {}
            for key in list(value.keys()):
                if self.params.get(key, None):
                    # self.params.get(key, None)是key, value.get(key)是value
                    # 创建键值对示例：data['parthan'] = 'Ubuntu'
                    # so，updatevalue[self.params.get(key, None)] = value.get(key)及为
                    # 例: { Proxy.score: 233}
                    updatevalue[self.params.get(key, None)] = value.get(key)

                    # 更新字段内容
            updateNum = query.update(updatevalue)
            self.session.commit()
        else:
            updateNum = 0
        return {'updateNum': updateNum}

    def select(self, count=None, conditions=None):
        if conditions:
            condition_list = []
            for key in list(conditions.keys()):
                if self.params.get(key, None):
                    condition_list.append(self.params.get(key) == conditions.get(key))
            conditions = condition_list
        else:
            conditions = []
        # 默认查询ip，端口，分数
        query = self.session.query(Proxy.ip, Proxy.port, Proxy.score)

        # 如查询条件存在，查询数量存在，对内容过滤后，倒排查询，
        if len(conditions) > 0 and count:
            for condition in conditions:
                query = query.filter(condition)
                # order_by排序查询,desc倒序查询，limit最多count条，all()全部数据
            return query.order_by(Proxy.score.desc(), Proxy.speed).limit(count).all()
        elif count:
            return query.order_by(Proxy.score.desc(), Proxy.speed).limit(count).all()
        # 如果查询数据数量不存在，输出所有查询到的内容
        elif len(conditions) > 0:
            for condition in conditions:
                query = query.filter(condition)
            return query.order_by(Proxy.score.desc(), Proxy.speed).all()
        else:
            return query.order_by(Proxy.score.desc(), Proxy.speed).all()

    def close(self):
        pass


if __name__ == '__main__':
    pass
    # try:
    #     sql = Sql()
    #     sql.init_db()
    # except Exception as e:
    #     print(e)
    #     raise Con_DB_Fail
    # proxy = {'ip': '192.168.1.1', 'port': int('80'), 'types': 0, 'protocol': 0, 'country': u'中国', 'area': u'四川',
    #          'speed': 0}
    # sql.insert(proxy)
    # proxy = {'ip': '192.168.1.1', 'port': 80, 'types': 0, 'protocol': 0, 'country': '中国', 'area': '广州', 'speed': 11.123}
    # sql.insert(proxy)
    # proxy = {'ip': '192.168.1.1', 'port': 88, 'types': 0, 'protocol': 0, 'country': '中国', 'area': '广州', 'speed': 11.123}
    # sql.insert(proxy)
    # sql.update({'ip': '192.168.1.1', 'port': 80}, {'score': 10})
    # print(sql.delete({'area': '广州', 'port': 88}))
    # print(sql.update({'port': '88'}, {'score': 233}))
    # print(sql.select(conditions={'ip': '192.168.1.1'}))
