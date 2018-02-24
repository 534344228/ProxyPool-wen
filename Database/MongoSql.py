import pymongo
from config import DB_CONFIG, DEFAULT_SCORE
from Database.ISQL import Isql


class MongoSql(Isql):
    def __init__(self):
        # 得到mongoclient，传入mongodb信息
        self.client = pymongo.MongoClient(DB_CONFIG['DB_CONNECT_STRING'], connect=False)

    def init_db(self):
        # 选择数据库.proxy
        self.db = self.client.test
        # 连接远程数据库，验证信息
        self.db.authenticate(name=DB_CONFIG['DB_CONNECT_NAME'], password=DB_CONFIG['DB_CONNECT_PWD'])
        # 选择集合，赋值给类属性
        self.db_proxys = self.db.proxys
        self.proxys = self.db_proxys

    def drop_db(self):
        self.client.drop_database(self.db)

    def insert(self, value=None):
        # 传入字典，后直接用命令写入
        if value:
            proxy = dict(ip=value['ip'], port=value['port'], types=value['types'],
                         protocol=value['protocol'], contry=value['country'], area=value['area'],
                         speed=value['speed'], score=DEFAULT_SCORE)
            res = self.proxys.insert_one(proxy)
            print("插入ID : {}".format(res.inserted_id))
    def delete(self, conditions=None):
        # 传入要删除的字典信息，世界使用命令
        if conditions:
            self.proxys.remove(conditions)
            return ('DeleteNum', 'OK')
        else:
            return ('DeleteNum', 'None')

    def update(self, conditions=None, value=None):
        # condition是查找内容，value修改内容
        # 例：update({"UserName":"www"},{"$set":{"Emial":"wen@126.com","password":"123"}})
        if conditions and value:
            res = self.proxys.update_many(conditions, {"$set": value})
            print("匹配成功次数 : {}".format(res.matched_count))  # matched_count 匹配次数
            print("成功次数 : {}".format(res.modified_count))  # modified_count 成功次数
            return {'UpdateNum': 'OK'}
        else:
            return {"UpdateNum": 'Fail'}

    def select(self, count=None, conditions=None):
        if count:
            count = int(count)
        else:
            count = 0
        # 筛选条件存在，并确保为字典格式
        if conditions:
            conditions = dict(conditions)
            if 'count' in conditions:
                # 如果count存在字典中就删除对应的内容
                del conditions['count']
                # 确保value为int类型
            conditions_name = ['types', 'protocol']
            # 通过得到传入字典参数中类型和协议的值
            for condition_name in conditions_name:
                value = conditions.get(condition_name, None)
                if value:
                    # 重新对字典key，value赋值，保证value为int类型
                    conditions[condition_name] = int(value)

            else:
                conditions = {}
            # limit为数量
            # sort排序：
            #   ASCENDING，升序
            #   DESCENDING，降序
            items = self.proxys.find(conditions, limit=count).sort([('speed', pymongo.ASCENDING), ('score', pymongo.DESCENDING)])
            # 声明res数组得到内容，并返回
            results = []
            for item in items:
                result = (item['ip'], item['port'], item['score'])
                results.append(result)
            return results
