# -*- utf-8 -*-
import os
import time
import datetime
import requests
import re
import json
from bs4 import BeautifulSoup
import pymysql
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from concurrent.futures import ProcessPoolExecutor

# 配置
CONFIG = {
    # 代理
    'proxy': {
        'https': 'http://127.0.0.1:7890'
    },
    # 抓取最近几页（个别站点不支持）
    'latestPages': 3,

    # 图片存放目录，可随意选择目录存放
    'imgDir': '../statics',
    # 图片url访问路径，需跟nginx配置保持一致
    'imgUrl': '/statics',
    # 定时任务
    'cron': '0 */1 * * 1-5',

    # 网络请求重试次数
    'retry': 3,
    # 反爬拦截之后等待时间（单位s）
    'wait': 2,

    # 抓取日志，分割flask日志，方便排查问题
    'logFile': 'spider.log',
    # 是否在屏幕同时打印抓取日志，True是，False否
    'screenLOG': True,

    # mysql
    'mysqlHost': '172.31.1.1',
    'mysqlPort': 3306,
    'mysqlDB': 'SRK',
    'mysqlUser': 'root',
    'mysqlPassword': 'Devops-Db;2021',
}

userAgent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'


# 公共函数
# 日志
def LOG(msg: str, prefix: str = '[ General ]'):
    """
    :param prefix:   前缀用于标识那个平台
    :param msg:     具体信息
    :return:
    """

    if CONFIG.get('screenLOG'):
        # 控制台打印
        print(time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time())), prefix, msg)

    with open(CONFIG.get('logFile'), "a") as f:
        # 写入日志文件
        print(time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time())), prefix, msg, file=f)
        f.close()


# 文件下载
def download(fileName: str, url: str):
    """
    :param fileName:    保存的文件名称
    :param url:         下载地址
    :return:
    """

    # 获取数据
    result = doGET(url=url, urlType='img')

    # 判断图片数据是否获取成功
    if not isinstance(result, bytes):
        return False

    # 存储文件
    try:
        with open(fileName, 'wb') as f:
            f.write(result)
            f.close()

    # 图片保存失败异常处理
    except Exception as e:
        LOG(msg="图片保存失败：%s" % e)
        return False

    # 正常返回
    return True


# 执行网络请求func
def doGET(url: str, headers: dict = None, urlType: str = None):
    """
    :param urlType:  请求的url类型，用于判断是否为图片
    :param url:    请求路径
    :param headers:    请求头
    :return:
    """

    # 基本参数配置
    proxy = CONFIG.get('proxy')

    # 预定义返回数据
    res = None

    # 请求接口
    try:
        if headers is not None:
            res = requests.get(url=url, headers=headers, proxies=proxy)
        else:
            res = requests.get(url=url, proxies=proxy)

    # 接口请求异常处理
    except Exception as e:
        LOG(msg="网络请求第一层异常捕获: %s" % e)
        # 网络请求失败则重试几次
        for i in range(CONFIG.get('retry')):
            # 开始前先等待1s
            time.sleep(CONFIG.get('wait'))
            # 继续捕获重试异常
            try:
                res = requests.get(url=url, headers=headers, proxies=proxy)
                # 请求成功则退出
                if res is not None:
                    break
            # 重试的过程中如果仍然请求失败，直接忽略
            except Exception as e:
                LOG(msg="路径 [ %s ], %s" % (url, e))
                pass
        # 再次判断
        if res is None:
            LOG(msg="路径 [ %s ] 详情抓取失败，请重新运行程序"
                    "或者联系管理员核实" % url)
            exit(1)

    # 判断下载类型
    if urlType is None:
        return res.content.decode('utf-8')
    # 如果为文件，则不进行解码
    return res.content


# 判断图片是否已经下载
def is_exists_img(imgName: str):
    return os.path.exists(imgName)


# 数据库操作类
class DB(object):
    """
    数据库操作类
    """

    # 日志前缀
    prefix = '[ DB ]'

    # 连接mysql
    def __init__(self):

        # 返回连接对象
        self.db = pymysql.connect(
            host=CONFIG.get('mysqlHost'),
            user=CONFIG.get('mysqlUser'),
            port=CONFIG.get('mysqlPort'),
            password=CONFIG.get('mysqlPassword'),
            database=CONFIG.get('mysqlDB'),
        )

    # 查询数据库
    def queryDB(self, sql: str):
        """
        :param sql:     查询语句
        :return:
        """

        # 获取游标
        cursor = self.db.cursor()

        # 执行
        try:

            # 执行sql 获取结果
            cursor.execute(sql)
            result = cursor.fetchall()

            # 提交事务关闭游标
            self.db.commit()
            cursor.close()

            return result

        except Exception as e:
            LOG(prefix=self.prefix, msg="[ 数据查询异常 ]: %s" % e)
            self.db.commit()
            cursor.close()
            return False

    # 通用插入方法
    def insertDB(self, sql: str):
        """
        :param sql:     插入语句
        :return:
        """

        # 获取游标
        cursor = self.db.cursor()

        try:
            cursor.execute(sql)
            self.db.commit()
        except Exception as e:
            LOG(prefix=self.prefix, msg="[ 数据写入异常 ]: %s" % e)

        cursor.close()

    # 保存抓取数据
    def saveData(self, table: str, values: list):
        """
        :param table:   表名称
        :param values:  值列表
        :return:
        """

        # 获取游标
        cursor = self.db.cursor()

        # 列表生成式，生成正确的sql
        sql = "INSERT INTO %s" \
              "(title, sub_title, img, content, exchange, lang, source_url, created) " \
              "VALUE (%s)" % (table, ','.join(["'%s'" % str(item) for item in values]))

        try:
            cursor.execute(sql)
            self.db.commit()
        except Exception as e:
            LOG(prefix=self.prefix, msg="[ 数据写入异常 ]: %s" % e)

        cursor.close()

    # 关闭连接
    def closeDB(self):
        """
        :return:
        """
        self.db.close()


# 新闻采集类
class ML(object):
    """
    马来西亚抓取类
    """
    # 基础参数
    base_url = "https://theedgemalaysia.com/"
    prefix = '[ ML ]'
    # 配置headers
    headers = {
        'Referer': 'https://theedgemalaysia.com/',
        'User-Agent': userAgent
    }

    # 中文判断
    def is_chinese(self, string: str):
        """
        :param string:   传入需要检测的字符串
        :return:
        """
        # 循环字符串，只要存在一个中文则判定为中文
        for char in string:
            if '\u0e00' <= char <= '\u9fa5':
                return True
        return False

    # 核心抓取函数
    def spider(self):
        """
        :return:
        """

        # 获取mysql对象
        db = DB()
        # 每页条数
        limit = 10

        # 循环页数
        for page in range(CONFIG.get('latestPages')):

            # 定义offset: 从第几条开始查询
            offset = page * limit
            # 获取需要抓取的新闻列表
            News = json.loads(self.getNeswIndex(offset=offset))['results']
            # 循环新闻列表
            for new in News:
                # 拼接新闻源地址
                sourceUrl = 'https://theedgemalaysia.com/node/%s' % new['nid']

                # 判断数据库中是否已经存储该新闻或者新闻是否为英文
                if not self.is_chinese(string=new['title']) or \
                        db.queryDB(sql='select source_url from news where source_url = "%s"' % sourceUrl) != ():
                    # 如果存在或者是英文直接跳过该新闻
                    continue

                # 定义需要存储的预数据
                values = [new['title'].replace('\'', '"'), new['summary'].replace('\'', '"')]

                # 下载图片
                # 判断该新闻有无图片
                if new['img'] != '':
                    imgName = '%s/%s.jpg' % (CONFIG.get('imgDir'), new['nid'])
                    # 判断该新闻的图片是否由其它语言已经下载过，
                    if is_exists_img(imgName) is False:
                        # 判断图片是否下载成功
                        if not download(fileName=imgName, url=new['img']):

                            saveStatus = False
                            # 如果未成功下载则重试两次
                            for i in range(CONFIG.get('retry')):
                                saveStatus = download(fileName=imgName, url=new['img'])
                                if saveStatus:
                                    break
                            # 判断最终图片是否保存成功
                            if not saveStatus:
                                LOG(prefix=self.prefix, msg="文章 [ %s ] 中的图片下载失败，请重新运行程序"
                                                            "或者联系管理员核实" % new['source_url'])
                                exit(1)

                    # 组装图片数据
                    values.append('%s/%s.jpg' % (CONFIG.get('imgUrl'), new['nid']))

                # 如果不存在图片
                else:
                    # 组装图片数据
                    values.append('')

                # 文章详情抓取
                content = self.getNewsDetails(path='node/%s' % new['nid']).replace('\'', '"')

                # 组装文章详情
                values.append(content)
                values.append('ml')
                values.append(new['language'])
                values.append(sourceUrl)
                values.append(new['created'] / 1000)

                # 插入mysql
                db.saveData(table='news', values=values)

            # 日志提示
            LOG(prefix=self.prefix, msg="第 [ %d ] 页抓取完成" % (page + 1))

        # 关闭数据库连接
        db.closeDB()

    # 文章页处理
    def getNewsDetails(self, path: str):
        """
        :param path:      文章页面路径
        :return:
        """

        # 请求获取页面数据
        page_source = doGET(url=self.base_url + path, headers=self.headers)

        # 判断数据是否正确
        if isinstance(page_source, dict):
            return False

        # 实例BS
        soup = BeautifulSoup(page_source, features="lxml")

        # 提取文章详情的核心主体
        pattern = re.compile(r'news-detail_newsTextDataWrap*')  # 正则模糊匹配
        newsBody = soup.find("div", attrs={"class": pattern})

        # 删除广告
        if newsBody.find("div", attrs={"class": "inPageAd"}) is not None:
            newsBody.find("div", attrs={"class": "inPageAd"}).decompose()

        # 删除底部英文链接
        if newsBody.find("em") is not None and 'version' in newsBody.find("em").text:
            newsBody.find("em").decompose()
            if newsBody.find("a") is not None:
                newsBody.find("a").decompose()
            newsBody.find_all("div", attrs={"class": "newsTextDataWrapInner"})[-1].decompose()

        # 返回最终数据
        return str(newsBody.contents[0])

    # 中文新闻列表
    def getNeswIndex(self, offset: int = 0):
        """
        :param offset:      起始位置, 默认为0, 也就是从最新的一条新闻开始获取
        :return:
        """

        # 定义接口path
        path = 'api/loadMoreCategories?offset=%d&categories=news' % offset

        # 执行请求
        return doGET(url=self.base_url + path, headers=self.headers)


# 马来西亚英文站新闻采集类
class MLEN(object):
    """
    马来西亚英文站新闻采集类
    """
    # 基础参数
    base_url = "https://theedgemalaysia.com/"
    prefix = '[ ML-EN ]'
    # 配置headers
    headers = {
        'Referer': 'https://theedgemalaysia.com/',
        'User-Agent': userAgent,
        'Accept-Language': 'en',
    }

    # 文章页处理
    def getNewsDetails(self, path: str):
        """
        :param path:      文章页面路径
        :return:
        """

        # 请求获取页面数据
        page_source = doGET(url=self.base_url + path, headers=self.headers)

        # 判断数据是否正确
        if isinstance(page_source, dict):
            return False

        # 实例BS
        soup = BeautifulSoup(page_source, features="lxml")

        # 提取文章详情的核心主体
        pattern = re.compile(r'news-detail_newsTextDataWrap*')  # 正则模糊匹配
        newsBody = soup.find("div", attrs={"class": pattern})

        # 删除广告
        if newsBody.find("div", attrs={"class": "inPageAd"}) is not None:
            newsBody.find("div", attrs={"class": "inPageAd"}).decompose()

        # 删除底部英文链接
        if newsBody.find("em") is not None and 'version' in newsBody.find("em").text:
            newsBody.find("em").decompose()
            if newsBody.find("a") is not None:
                newsBody.find("a").decompose()
            newsBody.find_all("div", attrs={"class": "newsTextDataWrapInner"})[-1].decompose()

        # 返回最终数据
        return str(newsBody.contents[0])

    # 英文新闻列表
    def getNeswIndex(self, offset: int = 0):
        """
        :param offset:      起始位置, 默认为0, 也就是从最新的一条新闻开始获取
        :return:
        """

        # 定义接口path
        path = 'api/loadMoreCategories?offset=%s&categories=malaysia' % offset

        # 执行请求
        return doGET(url=self.base_url + path, headers=self.headers)

    # 核心抓取函数
    def spider(self):
        """
        :return:
        """

        # 获取mysql对象
        db = DB()
        # 每页条数
        limit = 10

        # 循环页数
        for page in range(CONFIG.get('latestPages')):

            # 定义offset: 从第几条开始查询
            offset = page * limit
            # 获取需要抓取的新闻列表
            News = json.loads(self.getNeswIndex(offset=offset))['results']
            # 循环新闻列表
            for new in News:
                # 拼接新闻源地址
                sourceUrl = 'https://theedgemalaysia.com/node/%s' % new['nid']
                # # 判断数据库中是否已经存储该新闻或者新闻是否为英文
                if db.queryDB(sql='select source_url from news where source_url = "%s"' % sourceUrl) != ():
                    # 如果存在或者是英文直接跳过该新闻
                    continue

                # 定义需要存储的预数据
                values = [new['title'].replace('\'', '"'), new['summary'].replace('\'', '"')]

                # 下载图片
                # 判断该新闻有无图片
                if new['img'] != '':
                    imgName = '%s/%s.jpg' % (CONFIG.get('imgDir'), new['nid'])
                    # 判断该新闻的图片是否由其它语言已经下载过，
                    if is_exists_img(imgName) is False:
                        # 判断图片是否下载成功
                        if not download(fileName=imgName, url=new['img']):

                            saveStatus = False
                            # 如果未成功下载则重试两次
                            for i in range(CONFIG.get('retry')):
                                saveStatus = download(fileName=imgName, url=new['img'])
                                if saveStatus:
                                    break
                            # 判断最终图片是否保存成功
                            if not saveStatus:
                                LOG(prefix=self.prefix, msg="文章 [ %s ] 中的图片下载失败，请重新运行程序"
                                                            "或者联系管理员核实" % new['source_url'])
                                exit(1)

                    # 组装图片数据
                    values.append('%s/%s.jpg' % (CONFIG.get('imgUrl'), new['nid']))

                # 如果不存在图片
                else:
                    # 组装图片数据
                    values.append('')

                # 文章详情抓取
                content = self.getNewsDetails(path='node/%s' % new['nid']).replace('\'', '"')

                # 组装文章详情
                values.append(content)
                values.append('ml')
                values.append(new['language'])
                values.append(sourceUrl)
                values.append(new['created'] / 1000)

                # 插入mysql
                db.saveData(table='news', values=values)

                # 日志提示
            LOG(prefix=self.prefix, msg="第 [ %d ] 页抓取完成" % (page + 1))

        # 关闭数据库连接
        db.closeDB()


# 印度新闻采集类
class IDX(object):
    """
    印度抓取类
    """
    # 基础参数
    base_url = {
        "gujarati": "https://gujarati.moneycontrol.com",
        "en": "https://www.moneycontrol.com",
        "hindi": "https://hindi.moneycontrol.com"
    }
    prefix = '[ IDX ]'

    # 获取新闻列表
    def getNeswIndex(self, lang: str, page: int = 0):
        """
        :param lang:       抓取的语音类型
        :param page:      第几页
        :return:
        """

        # 定义接口path
        path = '/news/latest-news/page-%d' % page
        if lang == 'en':
            path = '/news/news-all/page-%d' % page

        # 根据语言选择Referer请求头
        headers = {
            'Referer': self.base_url[lang],
            'User-Agent': userAgent
        }

        # 执行请求
        res = doGET(url=self.base_url[lang] + path, headers=headers)

        # 定义文章列表
        newsList = []

        # 提取新闻map映射函数
        def bodyNewsMap(item):
            return item.find("a").attrs.get('href')

        # 清洗数据
        soup = BeautifulSoup(res, features='lxml')

        # 再次判断语言类型
        if lang == 'en':
            # 提取新闻列表DOM
            bodyNewsDom = soup.find("ul", attrs={"id": "cagetory"})
            bodyNewsDom = bodyNewsDom.find_all("h2")

            # 执行提取映射
            bodyNews = list(map(bodyNewsMap, bodyNewsDom))

        # 其他两种语言处理逻辑
        else:
            # 顶部第一条新闻
            try:
                topNews = soup.find("h2", attrs={"class": "topNews_h2"}).find("a").attrs.get('href')
                newsList.append(topNews)
            # 如果不存在头部新闻，则跳过
            except Exception:
                pass

            # 主体新闻列表
            pattern = re.compile(r'Category_cat-inn*')
            bodyNewsDom = soup.find_all("div", attrs={"class": pattern})

            # 执行提取映射
            bodyNews = list(map(bodyNewsMap, bodyNewsDom))

        # 返回新闻列表
        return newsList + bodyNews

    # 详情页处理
    def getNewsDetails(self, lang: str, path: str):
        """
        :param lang:     抓取的语音类型
        :param path:    具体新闻路径
        :return:
        """
        # 根据语言选择Referer请求头
        headers = {
            'Referer': self.base_url[lang],
            'User-Agent': userAgent
        }
        # 获取详情页DOM结构
        if lang == 'en':
            res = doGET(url=path, headers=headers)
        else:
            res = doGET(url=self.base_url[lang] + path, headers=headers)

        # 清洗数据
        soup = BeautifulSoup(res, 'lxml')

        # 再次判断语言，处理详情页特殊情况
        if lang == 'en':

            lft_body = soup.find("div", attrs={"class": "page_left_wrapper"})
            # 英文页主体
            context = lft_body.find("div", attrs={"id": "contentdata"})
            # 剔除广告
            for item in context.find_all("div"):
                item.decompose()
            # 提取发布时间

            date_str = re.split(r': ', lft_body.find("div", attrs={"class": "tags_last_line"}).text)[-1].strip()

        # 其他两种语言
        else:
            lft_body = soup.find("div", attrs={"class": "lft-side"})

            # 正则匹配文章详情主体
            pattern = re.compile(r'Article_body*')
            context = lft_body.find("div", attrs={"class": pattern})
            # 剔除广告
            if context.find("aside") is not None:
                context.find("aside").decompose()
            if context.find("a") is not None:
                context.find("a").parent.parent.decompose()
            # 剔除外围多余标签
            context = context.contents[0]

            # 提取发布时间
            pattern = re.compile(r'Tag_author_rgt*')
            date_str = re.split(r'[<>]', str(lft_body.find("div", attrs={"class": pattern}).find_all("p")[-1]))[-3]

        # 获取文章标题和子标体
        title = lft_body.find('h1').text
        sub_title = lft_body.find('h2').text
        # 获取图片链接
        img_url = ''
        if soup.find("meta", attrs={"property": "og:image"}) is not None:
            img_url = soup.find("meta", attrs={"property": "og:image"}).attrs.get('content')
        # 通用时间戳转换
        dateStamp = datetime.datetime.strptime(date_str.strip(), '%b %d, %Y %I:%M %p').timestamp()
        # 剔除JS文件
        if context.find("script") is not None:
            context.find("script").decompose()

        # 返回
        return {
            'title': title,
            'sub_title': sub_title,
            'img': img_url,
            'content': str(context),
            'created': dateStamp,
            'exchange': 'id',
            'lang': lang,
            'source_url': self.base_url[lang] + path if lang != 'en' else path  # 处理英文站路径包含域名的情况
        }

    # 核心抓取函数
    def spider(self):
        """
        :return:
        """

        # 获取mysql对象
        db = DB()

        # 循环页数
        for page in range(CONFIG.get('latestPages')):

            # 循环3种语言
            for lang, _ in self.base_url.items():

                # 获取需要抓取的新闻列表
                newsList = self.getNeswIndex(lang=lang, page=page + 1)

                # 循环新闻列表
                for newPath in newsList:

                    # 处理新闻直播和每日汇总，收费新闻，拍卖等情况
                    if bool(re.search(re.compile(r'news-live|moneycontrol-daily|news/videos|news/cricket'), newPath)):
                        continue

                    # 获取当前新闻的核心数据
                    # 通用特殊新闻处理
                    try:
                        new = self.getNewsDetails(lang=lang, path=newPath)
                    except Exception:
                        LOG(prefix=self.prefix, msg='[ %s ] 已跳过该类非正常新闻 [ %s ]' % (lang, newPath))
                        continue

                    # 拼接图片存储名称
                    imgNameBody = 'idx_%s' % re.split(r'[.-]', new.get('source_url'))[-2]

                    # 判断数据库中是否已经存储该新闻或者新闻是否为英文
                    if db.queryDB(
                            sql='select source_url from news where source_url = "%s"' % new.get('source_url')) != ():
                        # 如果存在直接跳过该新闻
                        continue

                    # 定义需要存储的预数据
                    values = [new['title'].replace('\'', '"'), new['sub_title'].replace('\'', '"')]

                    # 下载图片
                    # 判断该新闻有无图片
                    if new['img'] != '':
                        imgName = '%s/%s.jpg' % (CONFIG.get('imgDir'), imgNameBody)
                        # 判断图片是否下载成功
                        if not download(fileName=imgName, url=new['img']):

                            saveStatus = False
                            # 如果未成功下载则重试两次
                            for i in range(CONFIG.get('retry')):
                                saveStatus = download(fileName=imgName, url=new['img'])
                                if saveStatus:
                                    break
                            # 判断最终图片是否保存成功
                            if not saveStatus:
                                LOG(prefix=self.prefix, msg="文章 [ %s ] 中的图片下载失败，请重新运行程序"
                                                            "或者联系管理员核实" % new['source_url'])
                                exit(1)

                        # 组装图片数据
                        values.append('%s/%s.jpg' % (CONFIG.get('imgUrl'), imgNameBody))

                    # 如果不存在图片
                    else:
                        # 组装图片数据
                        values.append('')

                    # 文章详情抓取
                    content = new.get('content').replace('\'', '"')

                    # 组装文章详情
                    values.append(content)
                    values.append(new.get('exchange'))
                    values.append(new.get('lang'))
                    values.append(new.get('source_url'))
                    values.append(new.get('created'))

                    # 插入mysql
                    db.saveData(table='news', values=values)

                # 日志提示
                LOG(prefix=self.prefix, msg="[ %s ] 第 [ %d ] 页抓取完成" % (lang, page + 1))

        # 关闭数据库连接
        db.closeDB()


# 多进程类
class SPIDER(object):

    # 马来西亚抓取进程
    def ml(self):
        """
        :return:
        """

        # 定时任务
        sched = BlockingScheduler()
        # 中文站
        sched.add_job(ML().spider, CronTrigger.from_crontab(CONFIG.get('cron')), next_run_time=datetime.datetime.now())
        # 英文站
        sched.add_job(MLEN().spider, CronTrigger.from_crontab(CONFIG.get('cron')),
                      next_run_time=datetime.datetime.now())
        sched.start()

    # 印度抓取进程
    def idx(self):
        """
        :return:
        """

        # 定时任务
        sched = BlockingScheduler()
        sched.add_job(IDX().spider, CronTrigger.from_crontab(CONFIG.get('cron')), next_run_time=datetime.datetime.now())
        sched.start()

    # 进程池调度
    def run(self):
        """
        :return:
        """

        # 定义进程池
        p = ProcessPoolExecutor(2)

        # 加入任务
        p.submit(self.ml)
        p.submit(self.idx)

        # 阻塞主进程
        p.shutdown()


if __name__ == '__main__':
    SPIDER().run()
