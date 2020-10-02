# -*- coding: utf-8 -*-
"""
本项目fork自github: https://github.com/yeyeye777/wenshu_spider，并在其基础上增加和修改部分代码。
***因代理过期，未完整测试代码功能***
-------------------------------------------------
   File Name：     wenshu_retry.py
   Description :   根据年月日查询所有不同类型的案件
   Author :        LSQ
   date：          2020/9/30
-------------------------------------------------
   Change Activity:
                   2020/9/30: None
-------------------------------------------------
"""
import time
import datetime
import requests
import base64
# 随机user-agent
import fake_useragent
import random
import json
# 日志模块
import logging
import sys
# 多进程，分别负责案件列表和案件详情（没必要，真没必要。因为列表页最多只能获取5页，几秒钟就获取完了）
import multiprocessing

from Crypto.Cipher import DES3
# 重试模块
from tenacity import retry, retry_if_exception_type, wait_fixed
from lxml.etree import HTML
from pymongo import MongoClient

BS = DES3.block_size
UPDATE_PROXY = True
CURRENT_PROXY = {}


def pad(s):
    return s + (BS - len(s) % BS) * chr(BS - len(s) % BS)


def unpad(s):
    return s[0:-ord(s[-1])]


categories_dict = {
    'xsaj': "02",
    'msaj': "03",
    'zxaj': "10",
    'pcaj': "05",
    'xzaj': "04",
    'gxaj': "01",
    'qjsfxz': "06",
    'gjsfxz': "07",
    'fsbq': "09",
    'sfzc': "08",
    'qzqsypc': "11",
    'qt': "99",
}


class WenshuDocids(multiprocessing.Process):
    '''
    不要开启fiddler
    指定日期(格式：'2020-09-30')、案件类型，爬取案件文书
    '''

    def __init__(self, date=None, category=None, use_proxy=True):
        multiprocessing.Process.__init__(self)
        # 判断日期格式，如果格式不正确就报错(ValueError: time data '20200930' does not match format '%Y-%m-%d')
        time.strptime(date, '%Y-%m-%d')
        self.date = date
        # 判断类型是否存在，不存在就报错（self.category用于提交请求访问文书网的时候的queryCondition参数）
        self.category = categories_dict.get(category, 'error')
        if self.category == 'error':
            raise TypeError(f'{category} 该类型不存在。')
        # self.category_title用于访问数据库中的数据，如：
        # self.category_collection = self.mongo['wenshu'][self.category_title]，
        # 相当于self.category_collection = self.mongo['wenshu']['xsaj']
        self.category_title = category
        # 是否开启代理模式
        self.use_proxy = use_proxy

    def run(self):
        self.mongo = MongoClient('mongodb://127.0.0.1:27017')
        self.category_collection = self.mongo['wenshu'][self.category_title]
        logger.info('开始爬取id列表···')
        page_num = 0
        while 1:
            page_num += 1
            docid_list, total_page_num = self._get_doc_ids(page_num)
            if page_num >= 5:
                break
        self.mongo.close()
        logger.info('爬取完成。')

    @retry(retry=retry_if_exception_type(Exception), reraise=True)
    def _get_doc_ids(self, page_num):
        '''
        获取文章id列表
        1 发送请求，获取响应
        2 提取数据
        3 保存数据
        '''
        global UPDATE_PROXY
        global CURRENT_PROXY
        query_body = {
            "id": get_time(),
            "command": "queryDoc",
            "params": {
                "pageNum": page_num,
                "sortFields": "s50:desc",
                "ciphertext": generate_ciphertext(),
                "devtype": "1",
                # "app": "cpws",
                "devid": "",
                "pageSize": "200",
                "queryCondition": [{"key": "s8", "value": self.category}, {"key": "s48", "value": self.date}]
            }
        }
        # 1 发送请求(带请求体)，获取响应 {'ret': {'msg': '您已经被限制访问。', 'code': -12}, 'data': {}}
        url = 'http://wenshuapp.court.gov.cn/appinterface/rest.q4w'
        response_json = request(url, self.use_proxy, query_body)
        # 2 提取数据，获取文章id列表
        if response_json['ret']['code'] == 1 and response_json['ret']['msg'] == '':
            secret_key = response_json['data'].get('secretKey')
            encrypted_content = response_json['data'].get('content')
            decrypted_content = des.decrypt(encrypted_content, secret_key)
            content_json = json.loads(decrypted_content)
            docid_dict = dict()
            docid_dict['docids'] = list()
            if len(content_json.get('relWenshu')):
                for id in content_json.get('relWenshu'):
                    docid_dict['docids'].append(id)
                # 把案件文档列表的docids按照类型名称存入数据库(格式：{'_id':'2020-10-01','docids':['id****','id****'])
                docid_dict['_id'] = self.date
                if self.category_collection.count_documents({'_id': self.date}) == 0:
                    self.category_collection.insert_one(docid_dict)
                else:
                    docid_list = self.category_collection.find_one({'_id': self.date}).get('docids')
                    docid_list += docid_dict['docids']
                    self.category_collection.update_one({'_id': self.date}, {'$set': {'docids': docid_list}})
                # 获取总页数
                result_count = content_json['queryResult'].get('resultCount')
                total_page_num = result_count // 200 + 1
                logger.info(f'已完成爬取第{page_num}页的文书列表，共{total_page_num}页。最多爬取前5页。共可获得1000个docid。')
                return docid_dict['docids'], total_page_num
            else:
                logger.exception(f"{content_json}\n*****正在尝试重新获取文章id列表")
                UPDATE_PROXY = True
                time.sleep(5)
                raise Exception(f"{content_json}\n*****正在尝试重新获取文章id列表")
        else:
            logger.exception(f"{response_json['ret']}\n*****正在尝试重新获取文章id列表")
            UPDATE_PROXY = True
            time.sleep(5)
            raise Exception(f"{response_json['ret']}\n*****正在尝试重新获取文章id列表")


class WenshuDocDetail(multiprocessing.Process):
    mongo = MongoClient('mongodb://127.0.0.1:27017')

    def __init__(self, date=None, category=None, use_proxy=True):
        multiprocessing.Process.__init__(self)
        # 判断日期格式，如果格式不正确就报错(ValueError: time data '20200930' does not match format '%Y-%m-%d')
        time.strptime(date, '%Y-%m-%d')
        self.date = date
        # 判断类型是否存在，不存在就报错（self.category用于提交请求访问文书网的时候的queryCondition参数）
        self.category = categories_dict.get(category, 'error')
        if self.category == 'error':
            raise TypeError(f'{category} 该类型不存在。')
        # self.category_title用于访问数据库中的数据，如：
        # self.category_collection = self.mongo['wenshu'][self.category_title]，
        # 相当于self.category_collection = self.mongo['wenshu']['xsaj']
        self.category_title = category
        # 是否开启代理模式
        self.use_proxy = use_proxy

    def run(self):
        self.mongo = MongoClient('mongodb://127.0.0.1:27017')
        # 每一个文书的详细文档数据库
        self.collection = self.mongo['wenshu']['doc']
        # 单个类型的案件所有文书的集合
        self.category_collection = self.mongo['wenshu'][self.category_title]
        logger.info('开始爬取案件文章详情···')
        # docid_list = ''
        # result_cursor = self.category_collection.find({'_id': self.date})  # 也可直接用find_one方法
        # for cursor in result_cursor:
        #     docid_list = cursor.get('docids')
        docid_list = self.category_collection.find_one({'_id': self.date}).get('docids')
        for id in docid_list:
            count = self.collection.count_documents({'_id': id})
            if count == 0:
                doc = dict()
                doc_detail = self.__get_doc_detail(id)
                doc['_id'] = id
                doc['category'] = self.category
                doc['date'] = self.date
                doc['detail'] = doc_detail
                if doc_detail == '':
                    doc['detail'] = '不公开理由：人民法院认为不宜在互联网公布的其他情形'
                self.collection.insert_one(doc)
                logger.info(f'案件文档{id}写入成功···')
            else:
                logger.info(f'案件文档{id}已存在···')
        self.mongo.close()
        logger.info('案件文章详情爬取结束···')

    @retry(retry=retry_if_exception_type(Exception), reraise=True)
    def __get_doc_detail(self, id):
        '''
        获取文章内容
        1 加密请求体
        2 使用请求体发送请求
        3 提取文章详情
        :return: doc_detail
        '''
        # 1 使用base64加密请求体
        query_body = {
            "id": get_time(),
            "command": "docInfoSearch",
            "params": {
                # "app":"cpws",
                "ciphertext": "",  # self.__generate_ciphertext(),
                "docId": id,
                "devtype": "1",
                "devid": "c1396eb2bb2442129bd753827acd3460"
            }
        }
        # query_body = self.__generate_encrypted_body(query_body)
        # 2 使用请求体发送请求
        url = 'http://wenshuapp.court.gov.cn/appinterface/rest.q4w'
        response_json = request(url, self.use_proxy, query_body)
        # 3 提取文章详情
        if response_json['ret']['code'] == 1 and response_json['ret']['msg'] == '':
            secret_key = response_json['data'].get('secretKey')
            encrypted_content = response_json['data'].get('content')
            decrypted_content = des.decrypt(encrypted_content, secret_key)
            content_json = json.loads(decrypted_content)
            html_content = content_json['DocInfoVo'].get('qwContent')
            if html_content == '':
                return ''
            html = HTML(html_content)
            detail_list = html.xpath('//*/text()')
            if len(detail_list):
                data = ''.join(detail_list)
                return data
            # else代码块好像用处不大，一旦执行了即说明可能进入了retry死循环
            else:
                time.sleep(1)
                logger.info(f'文章详情无内容，正在尝试重新获取文章详情···')
                raise Exception(f'文章详情无内容，正在尝试重新获取文章详情···')
        else:
            return ''


def get_time():
    return datetime.datetime.now().strftime('%Y%m%d%H%M%S')


def generate_ciphertext():
    '''
    :return: ciphertext
    '''
    timestamp = str(round(time.time() * 1000))
    salt = ''.join(
        [random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/') for _ in range(24)])
    iv = datetime.datetime.now().strftime('%Y%m%d')
    enc = des.encrypt(timestamp, salt)
    strs = salt + iv + enc
    result = []
    for i in strs:
        result.append(bin(ord(i))[2:])
        result.append(' ')
    ciphertext = ''.join(result[:-1])
    return ciphertext


@retry(retry=retry_if_exception_type(requests.exceptions.ConnectTimeout), reraise=True)
def request(url, use_proxy, query_body):
    '''
    1 构造请求参数
    2 发送请求
    :param query_body: 请求体
    :return: 响应
    '''

    # 1 构造请求参数
    # 请求头
    global UPDATE_PROXY
    global CURRENT_PROXY
    headers = {
        'User-Agent': fake_useragent.UserAgent().random,
        # 'User-Agent': 'Dalvik/2.1.0 (Linux; U; Android 5.1.1; GM1900 Build/LYZ28N)',
        'Host': 'wenshuapp.court.gov.cn',
    }
    # 请求体的值(经过base64加密后的值)
    value = generate_encrypted_body(query_body)
    params = {
        'url': url,
        'headers': headers,
        'data': {
            'request': value,
        },
        'timeout': 10,
    }
    # 代理IP
    if use_proxy:
        # 判断CURRENT_PROXY是否为空，如果为空则获取新的代理IP，否则就使用已存在的代理IP地址。判断是否需要更新代理IP。
        if UPDATE_PROXY is True:
            proxies = get_proxies()
            params['proxies'] = proxies
            UPDATE_PROXY = False
        else:
            params['proxies'] = CURRENT_PROXY
    # 2 发送请求
    try:
        response = requests.request('POST', **params).json()
        if response['ret'].get('msg') != '':
            logger.exception(f'{response}\n*****更换IP重新请求。')
            UPDATE_PROXY = True
            raise Exception(f'{response}\n*****更换IP重新请求。')
        return response
    except (requests.exceptions.ConnectTimeout, Exception) as e:
        logger.exception(f'{e}\n*****request 请求出错。')
        UPDATE_PROXY = True


@retry(retry=retry_if_exception_type(Exception), reraise=True, wait=wait_fixed(10))
def get_proxies():
    params = {
        'appKey': '628801016187736064',
        'appSecret': 'hIBKU1Jc',
        'cnt': '1',
        'wt': 'json',
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36',
        'Host': 'api.xiaoxiangdaili.com',
    }
    response = requests.get('https://api.xiaoxiangdaili.com/ip/get', headers=headers, params=params).json()
    if response.get('code') == 200:
        ip = response['data'][0].get('ip')
        port = str(response['data'][0].get('port'))

        # username = ''
        # password = ''
        # proxies = {
        #     "http": "http://%(user)s:%(pwd)s@%(proxy)s/" % {"user": username, "pwd": password, "proxy": proxy},
        #     "https": "http://%(user)s:%(pwd)s@%(proxy)s/" % {"user": username, "pwd": password, "proxy": proxy}
        # }
        proxies = {
            'http': f'http://{ip}:{port}',
            'https': f'http://{ip}:{port}',
        }
        return proxies
    else:
        logger.exception(f'{response}*****请求代理IP太过频繁。')
        time.sleep(10)
        raise Exception(f'{response}*****正在重新获取新的代理IP···')


def generate_encrypted_body(query_body):
    return base64.standard_b64encode(str(query_body).encode('utf-8')).decode('utf-8')


class Des(object):

    @staticmethod
    def encrypt(text, key):
        """
        加密处理
        :param text:
        :param key:
        :return:
        """
        text = pad(text)
        iv = datetime.datetime.now().strftime('%Y%m%d').encode()
        cryptor = DES3.new(key, DES3.MODE_CBC, iv)
        # self.iv 为 IV 即偏移量
        x = len(text) % 8
        if x != 0:
            text = text + '\0' * (8 - x)  # 不满16，32，64位补0
        ciphertext = cryptor.encrypt(text.encode('utf-8'))
        return base64.standard_b64encode(ciphertext).decode("utf-8")

    @staticmethod
    def decrypt(text, key):
        """
        解密处理
        :param text:
        :param key:
        :return:
        """
        iv = datetime.datetime.now().strftime('%Y%m%d').encode()
        de_text = base64.standard_b64decode(text)
        cryptor = DES3.new(key, DES3.MODE_CBC, iv)
        plain_text = cryptor.decrypt(de_text)
        st = str(plain_text.decode("utf-8"))
        out = unpad(st)
        return out


class Logger(object):
    def __init__(self):
        # 获取logger对象
        self._logger = logging.getLogger()
        # 设置formart对象
        self.formatter = logging.Formatter(fmt='%(asctime)s %(filename)s [line:%(lineno)d] %(levelname)s: %(message)s',
                                           datefmt='%Y-%m-%d %H:%M:%S')
        # 设置日志输出
        self._logger.addHandler(self._get_file_handler('log.log'))
        self._logger.addHandler(self._get_console_handler())
        # 设置日志等级
        self._logger.setLevel(logging.INFO)

    def _get_file_handler(self, filename):
        '''
        获取文件日志handler
        :param filename: 文件名
        :return: filehandler
        '''
        # 实例化filehandler类
        filehandler = logging.FileHandler(filename=filename, encoding='utf-8')
        # 设置日志格式
        filehandler.setFormatter(self.formatter)
        return filehandler

    def _get_console_handler(self):
        '''
        获取终端日志handler
        :return: consolehandler
        '''
        # 实例化streamhandler类
        consolehandler = logging.StreamHandler(sys.stdout)
        # 设置日志格式
        consolehandler.setFormatter(self.formatter)
        return consolehandler

    @property
    def logger(self):
        return self._logger


logger = Logger().logger
des = Des()

if __name__ == '__main__':
    # wenshu_app = Wenshu(date=datetime.datetime.now().strftime('%Y-%m-%d'), category='xsaj', use_proxy=True)
    wenshu_docids = WenshuDocids(date='2020-10-01', category='xsaj', use_proxy=True)
    wenshu_docdetail = WenshuDocDetail(date='2020-10-01', category='xsaj', use_proxy=True)

    wenshu_docids.start()
    wenshu_docdetail.start()

    # 单独获取某一个案件内容（测试）
    # detail = wenshu_app._get_doc_detail('c04a469bcadb434d98b6ac45001448a8')
    # 浏览器打开链接https://wenshu.court.gov.cn/website/wenshu/181107ANFZ0BXSK4/index.html?docId=a65350a7227c4b35a55eac4601129f9a，检验爬取数据是否与浏览器相符。
