#sql接口类
class Isql(object):
    params = {'ip': None, 'port': None, 'types': None,
              'Protocol': None, 'country': None, 'area': None}

    def init_db(self):
        raise NotImplemented

    def drop_db(self):
        raise NotImplemented

    def insert(self, value=None):
        raise NotImplemented

    def delete(self, conditions=None):
        raise NotImplemented

    def update(self, conditions=None, value=None):
        raise NotImplemented

    def select(self, count=None, conditions=None):
        raise NotImplemented
