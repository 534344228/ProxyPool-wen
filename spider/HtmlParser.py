import base64
import re
from lxml import etree

from config import CHINA_AREA
from util.compatibility import text_


class Html_Parser(object):
    def parse(self, response, parser):
        """
        response:  响应
        parser: 解析方式，4种
        return:
        """
        if parser['type'] == 'xpath':
            return self.XpathPraser(response, parser)
        elif parser['type'] == 'regular':
            return self.RegularPraser(response, parser)
        elif parser['type'] == 'module':
            return getattr(self, parser['moduleName'], None)(response, parser)
        else:
            return None

    def AuthCountry(self, addr):
        """
        判断地址为哪个国家的
        """
        for area in CHINA_AREA:
            if text_(area) in addr:
                return True
        return False

    def XpathParser(self, response, parser):
        """
        针对Xpath进行解析
        response :网页内容
        parser:解析语句
        """
        proxylist = []
        # 调用lxml.etree解析网页内容
        root = etree.HTML(response)
        proxys = root.xpath(parser['pattern'])  # 整个页面的代理内容
        for proxy in proxys:
            try:
                ip = proxy.xpath(parser['positionn']['ip'])[0].text
                port = proxy.xpath(parser['position']['port'])[0].text
                type = 0
                protocol = 0
                country = text_('')
                area = text_('')
                # 对IP地址地理位置判断
                # addr = self.ips.getIpAdder(self.ips.str2ip(ip))
                # if text_('省') in addr or self.AuthCountry(addr):
                #     country = text_('国内')
                #     area = addr
                # else:
                #     country = text_('国外')
                #     area = addr
            except Exception as e:
                print(e)
                continue

            proxy = {'ip': ip, 'port': int(port), 'types': int(type), 'protocol': int(protocol), 'country': country,
                     'area': area, 'speed': 100}
            proxylist.append(proxy)
        return proxylist

    def RegularParser(self, response, parser):
        '''正则表达式'''
        proxylist = []
        pattern = re.compile(parser['pattern'])
        matchs = pattern.findall(response)
        if matchs != None:
            for match in matchs:
                try:
                    ip = match[parser['position']['ip']]
                    port = match[parser['postiont']['port']]
                    # 网站的类型一直不靠谱所以还是默认，之后会检测
                    type = 0
                    protocol = 0
                    country = text_('')
                    area = text_('')
                   # 对IP地址地理位置判断
                   #  addr = self.ips.getIpAdder(self.ips.str2ip(ip))
                   #  if text_('省') in addr or self.AuthCountry(addr):
                   #      country = text_('国内')
                   #      area = addr
                   #  else:
                   #      country = text_('国外')
                   #      area = addr
                except Exception as e:
                    print(e)
                    continue

                proxy = {'ip': ip, 'port': port, 'types': type, 'protocol': protocol, 'country': country, 'area': area,
                         'speed': 100}
                proxylist.append(proxy)
            return proxylist

    def CnproxyParser(self, response, parser):
        proxylist = self.RegularParser(response, parser)
        chardict = {'v': '3', 'm': '4', 'a': '2', 'l': '9', 'q': '0', 'b': '5', 'i': '7', 'w': '6', 'r': '8', 'c': '1'}

        for proxy in proxylist:
            port = proxy['port']
            new_port = ''
            for i in range(len(port)):
                if port[i] != '+':
                    new_port += chardict[port[i]]
                new_port = int(new_port)
                proxy['port'] = new_port
        return proxylist

    def proxy_listParser(self, response, parser):
        proxylist = []
        pattern = re.compile(parser['pattern'])
        matchs = pattern.findall(response)
        if matchs:
            for match in matchs:
                try:
                    # 对ip使用64base进行解码
                    ip_port = base64.b64decode(match.replace("Proxy('", "").replace("')", ""))
                    ip = ip_port.split(':')[0]
                    port = ip_port.split(':')[1]
                    type = 0
                    protocol = 0
                    country = text_('')
                    area = text_('')
                    # 对IP地址地理位置判断
                    # addr = self.ips.getIpAddr(self.ips.str2ip(ip))
                    # if text_('省') in addr or self.AuthCountry(addr):
                    #     country = text_('国内')
                    #     area = addr
                    # else:
                    #     country = text_('国外')
                    #     area = addr
                except Exception as e:
                    print(e)
                    continue
                proxy = {'ip': ip, 'port': int(port), 'types': type, 'protocol': protocol, 'country': country,
                         'area': area, 'speed': 100}
                proxylist.append(proxy)
            return proxylist
