import time
import gevent
import sys
import config

from gevent.pool import Pool
from gevent import monkey
monkey.patch_all()
from multiprocessing import Queue, Process, Value

from Database import DataStore
from Database.DataStore import sql
from spider.HtmlDownloader import Html_Downloader
from spider.HtmlParser import Html_Parser


def startProxyCrawl(queue, db_proxy_num, myip):
    crawl = ProxyCrawl(queue, db_proxy_num, myip)
    crawl.run()


class ProxyCrawl(object):
    proxies = set()  # 创建空集合

    def __init__(self, queue, db_proxy_num, myip):
        # 限制并发协程数量
        self.crawl_pool = Pool(config.THREADNUM)
        self.queue = queue
        self.db_proxy_num = db_proxy_num
        self.myip = myip

    def run(self):
        while True:
            self.proxies.clear()
            str = 'IPProxyPool----->>>>>>>>beginning'
            sys.stdout.write(str + '\r\n')
            sys.stdout.flush()
            # 1.从数据库取数据,检查已经存在的ip代理
            proxylist = sql.select()
            """
            协程的作用，是在执行函数A时，可以随时中断，去执行函数B，然后中断继续执行函数A（可以自由切换）。
            但这一过程并不是函数调用（没有调用语句），这一整个过程看似像多线程，然而协程只有一个线程执行。
            """
            spawns = []
            for proxy in proxylist:
                # gevent.spawn会创建一个新的greenlet协程对象，并运行它
                spawns.append(gevent.spawn(detect_from_db, self.myip, proxy, self.proxies))
                if len(spawns) >= config.MAX_CHECK_CONCURRENT_PER_PROCESS:
                    # gevent.joinall()会等待所有传入的greenlet协程运行结束后再退出，timeout参数来设置超时时间，单位是秒
                    gevent.joinall(spawns)
                    spawns = []
            gevent.joinall(spawns)
            self.db_proyx_num.value = len(self.proxies)
            str = 'IPProxyPool----->>>>>>>>db exists ip:%d' % len(self.proxies)

            # 2.ip代理数量小于最小数量
            if len(self.proxies) < config.MINNUM:
                str += '\r\nIPProxyPool----->>>>>>>>now ip num < MINNUM,start crawling...'
                sys.stdout.write(str + "\r\n")
                sys.stdout.flush()
                spawns = []
                for p in config.parserList:
                    spawns.append(gevent.spawn(self.crawl, p))
                    if len(spawns) >= config.MAX_DOWNLOAD_CONCURRENT:
                        gevent.joinall(spawns)
                        spawns = []
                gevent.joinall(spawns)
            else:
                str += '\r\nIPProxyPool----->>>>>>>>now ip num meet the requirement,wait UPDATE_TIME...'
                sys.stdout.write(str + "\r\n")
                sys.stdout.flush()

            time.sleep(config.UPDATE_TIME)

    def crawl(self, parser):
        html_parser = Html_Parser()
        for url in parser['urls']:
            response = Html_Downloader.download(url)
            if response is not None:
                proxylist = html_parser.parse(response, parser)
                if proxylist is not None:
                    for proxy in proxylist:
                        proxt_str = '%s:%s' % (proxy['ip'], proxy['port'])
                        if proxt_str not in self.proxies:
                            self.proxies.add(proxt_str)
                            while True:
                                if self.queue.full():
                                    time.sleep(0.1)
                                else:
                                    self.queue.put(proxy)
                                    break


if __name__ == "__main__":
    # 进程间数据共享
    # 共享内存有两种结构 一个是Value,一个是Array，都在内部实现了锁机制，因此多进程安全
    # i是int类型,d是double类型
    DB_PROXY_NUM = Value('i', 0)
    # queue 共享的进程队列
    q1 = Queue()
    q2 = Queue()
    # Process(taget=函数名,args=函数参数)
    p1 = Process(target=startProxyCrawl, args=(q1, DB_PROXY_NUM))
    p2 = Process(target=validator, args=(q1, q2))
    p3 = Process(target=DataStore.store_data, args=(q2, DB_PROXY_NUM))
    # start生成进程
    p1.start()
    p2.start()
    p3.start()
