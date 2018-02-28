from multiprocessing import Value, Queue, Process
from Database.DataStore import store_data

from validator.Validator import validator, getMyIP
from spider.ProxyCrawl import startProxyCrawl

from config import TASK_QUEUE_SIZE

if __name__ == "__main__":
    myip = getMyIP()
    # Value是multiprocessing的共享变量，此处初始化为int，值为0
    DB_PROXY_NUM = Value('i', 0)
    # multiprocessing.queue跨进程通信队列
    q1 = Queue(maxsize=TASK_QUEUE_SIZE)
    q2 = Queue()
    p1 = Process(target=startProxyCrawl, args=(q1, DB_PROXY_NUM, myip))
    p2 = Process(target=validator, args=(q1, q2, myip))
    p3 = Process(target=store_data, args=(q2, DB_PROXY_NUM))
    p1.start()
    p2.start()
    p3.start()
    p1.join()
    p2.join()
    p3.join()
