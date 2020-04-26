import requests as rq
import re
import datetime
import time
from urllib.parse import unquote, urlparse, urlunparse
import pymongo
from html import unescape
from multiprocessing import Pool as ProcessPool
from multiprocessing.dummy import Pool as ThreadPool
from bs4 import BeautifulSoup

# pymongo.MongoClient().drop_database('baidubaike')
tasks = pymongo.MongoClient().baidubaike.tasks  # 将队列存于数据库中
items = pymongo.MongoClient().baidubaike.items  # 存放结果
tasks.create_index([('url', 'hashed')])  # 建立索引，保证查询速度
items.create_index([('url', 'hashed')])
count = items.count()  # 已爬取页面总数
if tasks.count() == 0:  # 如果队列为空，就把该页面作为初始页面，这个页面要尽可能多超链接
    u = 'http://baike.baidu.com/item/python'
    if not items.find_one({'url': u}):
        tasks.insert({'url': u})

url_split_re = re.compile('[&+]')


def clean_url(url):
    url = urlparse(url)
    return url_split_re.split(urlunparse((url.scheme, url.netloc, url.path, '', '', '')))[0]


header = {
    "User-Agent":
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/81.0.4044.122 Safari/537.36'}


def run():
    def main():
        global count
        while tasks.count() > 0:
            url = tasks.find_one_and_delete({})['url']  # 取出一个url，并且在队列中删除掉
            try:
                sess = rq.get(url, headers=header)
            except:
                continue
            web = sess.content.decode('utf-8', 'ignore')
            urls = re.findall('href="(/item/.*?)"', web)  # 查找所有站内链接
            for u in urls:
                try:
                    u = unquote(str(u))
                except:
                    continue
                u = 'http://baike.baidu.com' + u
                u = clean_url(u)
                if not items.find_one({'url': u}):  # 把还没有队列过的链接加入队列
                    tasks.update({'url': u}, {'$set': {'url': u}}, upsert=True)
            soup = BeautifulSoup(web)
            text = soup.find_all('div', class_='para')
            # 爬取我们所需要的信息，需要正则表达式知识来根据网页源代码而写
            if text:
                text = ' '.join(
                    [re.sub('[ \n\r\t\u3000]+', ' ', re.sub('<.*?>|\xa0', '', unescape(str(t))).strip()) for t in
                     text])  # 对爬取的结果做一些简单的处理
                title = re.findall(u'<title>(.*?)_百度百科</title>', str(soup.title))[0]
                items.update({'url': url}, {'$set': {'url': url, 'title': title, 'text': text}}, upsert=True)
                count += 1
                print('%s, 爬取《%s》，URL: %s, 已经爬取%s' % (datetime.datetime.now(), title, url, count))

    p = ThreadPool(4, main)  # 多线程爬取，4是线程数
    p.close()
    p.join()


if __name__ == '__main__':
    pool = ProcessPool(4, run)  # 多进程爬取，4是进程数
    pool.close()
    pool.join()
    time.sleep(30)
    pool.terminate()
