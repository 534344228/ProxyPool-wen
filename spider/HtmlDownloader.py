import random
import config
from Database.DataStore import sql

import requests
import chardet


class Html_Downloader(object):
    @staticmethod  # staticmethod不需要表示自身对象的self和自身类的cls参数，就跟使用函数一样
    def download(url):
        try:
            r = requests.get(url=url, headers=config.get_header(), timeout=config.TIMEOUT)
            # print("应答码:{}".format(r))
            # 将获取内容的编码更改；通过chardet.detect检查内容，并拿到编码方式
            r.encoding = chardet.detect(r.content)['encoding']
            # 检测是否获取成功
            if (not r.ok) or len(r.content) < 500:
                raise ConnectionError
            else:
                return r.text
        except Exception as e:
            print(e)
            count = 0  # 重试次数
            # 获取代理IP再进行下载
            proxylist = sql.select(10)
            if not proxylist:
                return None

            while count < config.RETRY_TIME:
                try:
                    proxy = random.choice(proxylist)
                    ip = proxy[0]
                    port = proxy[1]
                    proxies = {'http': 'http://%s%s' % (ip, port), 'https': 'http://%s:%s' % (ip, port)}

                    r = requests.get(url=url, headres=config.get_header(), timeout=config.TIMEOUT, proxies=proxies)
                    r.encoding = chardet.detect(r.content)['encoding']
                    if (not r.ok) or len(r.content) < 500:
                        raise ConnectionError
                    else:
                        return r.text
                except Exception:
                    count += 1
        return None
