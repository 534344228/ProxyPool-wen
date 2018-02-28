from spider.HtmlDownloader import Html_Downloader
from spider.HtmlParser import Html_Parser


def crawl(parser):
    html_parser = Html_Parser()
    for url in parser['urls']:
        response = Html_Downloader.download(url)
        print("下载URl页面:{}".format(url))
        if response is not None:
            proxylist = html_parser.parse(response, parser)
            print("代理列表: {}".format(proxylist))
            if proxylist is not None:
                for proxy in proxylist:
                    proxt_str = '%s:%s' % (proxy['ip'], proxy['port'])
                    print(proxt_str)


def crawl_alone(parser):
    html_parser = Html_Parser()
    url = 'http://www.66ip.cn/areaindex_1/1.html'
    res = Html_Downloader.download(url)
    if res:
        proxylist = html_parser.parse(res, parser)
        print(proxylist)
        for proxy in proxylist:
            proxt_str = 'ip=%s:port=%s' % (proxy['ip'], proxy['port'])
            print(proxt_str)


if __name__ == '__main__':
    xpath = {
        'urls': ['http://www.66ip.cn/areaindex_%s/%s.html' % (m, n) for m in range(1, 35) for n in range(1, 10)],
        'type': 'xpath',
        'pattern': ".//*[@id='footer']/div/table/tr[position()>1]",
        'position': {'ip': './td[1]', 'port': './td[2]', 'type': './td[4]', 'protocol': ''}
    }
    module = {
        'urls': ['https://proxy-list.org/english/index.php?p=%s' % n for n in range(1, 10)],
        'type': 'module',
        'moduleName': 'proxy_listParser',
        'pattern': 'Proxy\(.+\)',
        'position': {'ip': 0, 'port': -1, 'type': -1, 'protocol': 2}

    }
    cn = {
        'urls': ['http://www.cnproxy.com/proxy%s.html' % i for i in range(1, 11)],
        'type': 'module',
        'moduleName': 'CnproxyParser',
        'pattern': r'<tr><td>(\d+\.\d+\.\d+\.\d+)<SCRIPT type=text/javascript>document.write\(\"\:\"(.+)\)</SCRIPT></td><td>(HTTP|SOCKS4)\s*',
        'position': {'ip': 0, 'port': 1, 'type': -1, 'protocol': 2}
    }

    # crawl(module)
    # crawl_alone(xpath)
    crawl(cn)
