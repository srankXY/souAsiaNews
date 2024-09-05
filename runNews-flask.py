# -*- utf-8 -*-

import time
import datetime
import requests
import re
import json
import functools
from bs4 import BeautifulSoup
import pymysql
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import flask
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

# 配置
CONFIG = {
    # 代理
    'proxy': {
        'https': 'http://127.0.0.1:7890'
    },
    # 图片存放目录，可随意选择目录存放
    'imgDir': './statics',
    # 图片url访问路径，需跟nginx配置保持一致
    'imgUrl': '/statics',
    # 定时任务
    'cron': '*/5 * * * *',

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


# Flask API
api = flask.Flask(__name__)
api.json.ensure_ascii = False


# 公共函数
# 日志
def LOG(msg):
    """
    :param msg:     具体信息
    :return:
    """

    if CONFIG.get('screenLOG'):
        # 控制台打印
        print(time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time())), msg)

    with open(CONFIG.get('logFile'), "a") as f:
        # 写入日志文件
        print(time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time())), msg, file=f)
        f.close()


# 通用异常处理，装饰器
def generalTryCatch(func):
    """
    通用异常装饰器
    :param func:    传入具体的函数名称以装饰该函数
    :return:
    """

    # 保留被装饰函数原始元数据
    @functools.wraps(func)
    # 装饰函数
    def wrapper(*args, **kwargs):

        # 异常捕获
        try:
            # 执行实际函数
            return func(*args, **kwargs)
        # 实际函数通用出错处理
        except Exception as e:
            # 调试打印
            print("函数%s, 出现异常： %s" % (func.__name__, e))
            # 异常返回
            return response(code=8500, result={
                'type': "错误",
                'data': "API调用失败，请联系管理员核实"
            })
    # 返回装饰函数
    return wrapper


# 返回框架
def response(code=8200, **kwargs):
    """
    :param code:    响应状态码
    :param kwargs:  返回的具体数据，示例：{'type': '盘中k线','symbol': 'LEE','data': result}，result为具体的k线数据
    :return:
    """

    data = {
        'code': code,
        'timestamp': time.time()
    }

    # 根据传参重组返回数据
    for k, v in kwargs['result'].items():
        data[k] = v

    return data


# 数据库操作类
class DB(object):
    """
    数据库操作类
    """

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
            LOG(msg="[ 数据查询异常 ]: %s" % e)
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
            LOG(msg="[ 数据写入异常 ]: %s" % e)

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
              "(nid, title, sub_title, img, content, lang, source_url, created) " \
              "VALUE (%s)" % (table, ','.join(["'%s'" % str(item) for item in values]))

        try:
            cursor.execute(sql)
            self.db.commit()
        except Exception as e:
            LOG(msg="[ 数据写入异常 ]: %s" % e)

        cursor.close()

    # 关闭连接
    def closeDB(self):
        """
        :return:
        """
        self.db.close()


# 新闻采集类
class COLLECT(object):
    """
    抓取类
    """

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

    # 入口函数
    def MAIN(self):
        """
        :return:
        """

        # 获取数据库数据条数
        # 获取mysql对象
        db = DB()
        # 获取数据库已有新闻条数
        saveCount = db.queryDB(sql="select total from spiderLimit")[0][0]

        # 获取 新闻 总数
        newsTotal = int(json.loads(self.getNeswIndex())['total'])

        # 判断是否存在未抓取的新闻进行循环
        while newsTotal > saveCount:
            # 抓取数据
            self.spider(newsTotal=newsTotal, saveCount=saveCount)

            time.sleep(CONFIG.get('wait'))
            # 更新nesTotal，saveCount
            saveCount = db.queryDB(sql="select total from spiderLimit")[0][0]
            newsTotal = int(json.loads(self.getNeswIndex())['total'])

        # 已经抓完的情况
        LOG(msg='[ 完成 ] 当前新闻总数 [ %d ] 条， 已抓取 [ %d ] 条' % (newsTotal, saveCount))

        # 关闭mysql连接
        db.closeDB()

    # 核心抓取函数
    def spider(self, newsTotal: int, saveCount: int):
        """
        :param newsTotal:    新闻总数
        :param saveCount:    已经抓取的新闻总数
        :return:
        """

        # 获取mysql对象
        db = DB()
        # 计算 新闻列表 API offset，目的是从最老的新闻开始获取
        # 定义 API 每次返回的条数，也是需要抓取的条数
        limit = 10
        offset = newsTotal - saveCount - limit

        # 判断 未抓取新闻 不超过10条的情况
        if offset < 0:
            # 重新定义需要抓取的条数
            limit += offset
            # 重新定义 开始抓取的位置
            offset = 0
        LOG(msg='总数据为 [ %d ] 条，正在抓取第 [ %d - %d ] 条新闻' % (newsTotal, offset, offset + limit))

        # 计算已抓取总数
        saveCount += limit

        # 重新获取正确的需要抓取的新闻列表
        News = json.loads(self.getNeswIndex(offset=offset))['results']

        # 时间评估
        beginTime = time.time()
        # 循环抓取
        for new in News:

            # 判断limit 是否小于0，是说明已经抓取完毕，直接退出
            if limit <= 0:
                break

            # 判断新闻是否为英文
            if not self.is_chinese(string=new['title']):
                # 是英文直接跳过本次循环
                limit -= 1
                continue

            # 定义需要存储的预数据
            values = [new['nid'], new['title'].replace('\'', '"'), new['summary'].replace('\'', '"')]

            # 下载图片
            # 判断该新闻有无图片
            if new['img'] != '':
                imgName = '%s/%s.jpg' % (CONFIG.get('imgDir'), new['nid'])
                # 判断图片是否下载成功
                if not self.download(fileName=imgName, url=new['img']):

                    saveStatus = False
                    # 如果未成功下载则重试两次
                    for i in range(CONFIG.get('retry')):
                        saveStatus = self.download(fileName=imgName, url=new['img'])
                        if saveStatus:
                            break
                    # 判断最终图片是否保存成功
                    if not saveStatus:
                        LOG(msg="文章 [ %s ] 中的图片下载失败，请重新运行程序"
                                "或者联系管理员核实" % new['nid'])
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
            values.append(new['language'])
            values.append('https://theedgemalaysia.com/node/%s' % new['nid'])
            values.append(new['created'])

            # 插入mysql
            db.saveData(table='news', values=values)

            # 更新limit
            limit -= 1

        # 时间评估结束
        endTime = time.time()

        # 预估总时间
        if offset != 0:
            LOG(msg='还剩 [ %d ] 条新闻未抓取，预估时间为 [ %d ] 分钟' % (offset, offset / 10 * (endTime - beginTime) / 60))

        # 更新抓取总数
        db.insertDB(sql='update spiderLimit set total=%d' % saveCount)

        # 关闭数据库连接
        db.closeDB()

    # 文章页处理
    def getNewsDetails(self, path: str):
        """
        :param path:      文章页面路径
        :return:
        """

        # 请求获取页面数据
        page_source = self.doGET(path)

        # 判断数据是否正确
        if isinstance(page_source, dict):
            return False

        # 实例BS
        soup = BeautifulSoup(page_source, features="html.parser")

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
        return self.doGET(path)

    # 文件下载
    def download(self, fileName: str, url: str):
        """
        :param fileName:    保存的文件名称
        :param url:         下载地址
        :return:
        """

        # 获取数据
        result = self.doGET(url, urlType='img')

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
    def doGET(self, path: str, urlType: str = None):
        """
        :param urlType:  请求的url类型，用于判断是否为图片
        :param path:    请求路径
        :return:
        """

        # 基本参数配置
        base_url = "https://theedgemalaysia.com/"
        proxy = CONFIG.get('proxy')

        # 判断urlType类型
        if urlType is not None:
            base_url = ''

        # 配置headers
        headers = {
            'Referer': 'https://theedgemalaysia.com/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
        }

        # 预定义返回数据
        res = None

        # 请求接口
        try:
            res = requests.get(url=base_url + path, headers=headers, proxies=proxy)

        # 接口请求异常处理
        except Exception as e:
            LOG(msg="网络请求第一层异常捕获: %s" % e)
            # 网络请求失败则重试几次
            for i in range(CONFIG.get('retry')):
                # 开始前先等待1s
                time.sleep(CONFIG.get('wait'))
                # 继续捕获重试异常
                try:
                    res = requests.get(url=base_url + path, headers=headers, proxies=proxy)
                    # 请求成功则退出
                    if res is not None:
                        break
                # 重试的过程中如果仍然请求失败，直接忽略
                except Exception as e:
                    LOG(msg="路径 [ %s ], %s" % (path, e))
                    pass
            # 再次判断
            if res is None:
                LOG(msg="路径 [ %s ] 详情抓取失败，请重新运行程序"
                        "或者联系管理员核实" % path)
                exit(1)

        # 判断下载类型
        if urlType is None:
            return res.content.decode('utf-8')
        # 如果为文件，则不进行解码
        return res.content


# 通用新闻查询
def queryNews(sql: str):
    """
    :param sql: 需要执行的sql
    :return:
    """

    # 数据映射函数
    def newsDict(item):
        return {
            'nid': item[1],
            'title': item[2],
            'abstract': item[3],
            'img': item[4],
            'content': item[5],
            'lang': item[6],
            'source_url': item[7],
            'created': item[8]
        }

    # 获取数据库对象
    db = DB()
    # 执行sql
    result = db.queryDB(sql=sql)
    db.closeDB()

    if result != ():
        # 数据映射
        result = list(map(newsDict, result))

    # 返回
    return result


# 新闻查询接口
@api.route('/api/news', methods=['GET'])
@generalTryCatch
def getNews():
    """
    :param: begin 可选, 从什么位置开始查询，不包含， 默认为：0
    :param: limit 可选, 查询多少条， 默认为：10
    :return:
    """

    # 默认参数
    begin = 0
    limit = 10

    # 获取参数
    params = flask.request.args
    if 'begin' in params:
        begin = params.get('begin')

    if 'limit' in params:
        limit = params.get('limit')

    # 拼接sql
    sql = 'select * from news order by created desc limit %d, %d' % (int(begin), int(limit))

    # 返回数据
    return response(code=8200, result={
        'type': "新闻查询",
        'begin': begin,
        'limit': limit,
        'data': queryNews(sql=sql)
    })


# 新闻查询接口
@api.route('/api/newsCount', methods=['GET'])
@generalTryCatch
def newsCount():
    """
    :return:
    """

    # 获取数据库对象
    db = DB()

    # 拼接sql
    sql = 'select count(id) from news'

    # 执行sql
    result = db.queryDB(sql=sql)[0][0]
    db.closeDB()

    # 返回数据
    return response(code=8200, result={
        'type': "新闻总数查询",
        'data': {
            'newsCount': result
        }
    })


# 新闻筛选
@api.route('/api/filterNews', methods=['GET'])
@generalTryCatch
def filterNews():
    """
    :param:     接受条件参数，如: nid=66981, title='马股持续下跌'
    :return:
    """

    # 获取参数
    params = flask.request.args

    # 拼接sql
    sql = "select * from news where %s = %s" % (list(params.items())[0][0], list(params.items())[0][1])

    # 返回数据
    return response(code=8200, result={
        'type': "新闻筛选",
        'data': queryNews(sql=sql)
    })


# 多进程类
class MUILTIPROCESS(object):

    # 抓取子进程
    def spiderProcess(self):
        """
        :return:
        """

        # 定时任务
        sched = BlockingScheduler()
        sched.add_job(COLLECT().MAIN, CronTrigger.from_crontab(CONFIG.get('cron')), next_run_time=datetime.datetime.now())
        sched.start()

    # Flask 子进程
    def flaskProcess(self):
        """
        :return:
        """

        api.run(port=9999, debug=True, host='0.0.0.0', use_reloader=False)

    # 进程池调度
    def run(self):
        """
        :return:
        """

        # 定义进程池
        p = ProcessPoolExecutor(2)

        # 加入任务
        p.submit(self.spiderProcess)
        p.submit(self.flaskProcess)

        # 阻塞主进程
        p.shutdown()


if __name__ == '__main__':

    MUILTIPROCESS().run()
