
from spider.HtmlDownloader import Html_Downloader

url = 'http://www.66ip.cn/'
res = Html_Downloader.download(url)
print(res)