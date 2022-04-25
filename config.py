# -*- coding: utf-8 -*-
import os
import logging.config
import pandas as pd
import pymysql

import os

debug = False

if debug:
    # base_dir = os.path.join(os.path.join(base_dir, 'logs'))
    base_dir = os.path.split(os.path.realpath(__file__))[0]
else:
    base_dir = '/data/limeng/logs'

recent_ndays = 14

if debug:
    normal_brand_names = pd.read_csv(
        "C:\\Users\\29678\\Desktop\\voila_china\\find_sku_algorithm\\standardBrandName.csv", encoding='gbk')
else:
    normal_brand_names = pd.read_csv("find_sku_algorithm/standardBrandName.csv", encoding='utf-8-sig')


query_sentence = [{"stdSubCateName": "Clothing", "stdSubCate2Name": "Coats & Jackets"},
                  {"_id": 0, 'spuId': 1, 'title': 1, 'canonicalUrl': 1, 'maxMsrp': 1, 'siteName': 1,
                   'stdCateName': 1, 'stdSubCateName': 1, 'stdSubCate2Name': 1, 'brandName': 1, 'updatedUtc': 1}]

# 标题与价格的权重
title_weight = 0.4
jaccard_weight = 0.4
price_weight = 0.2

# 设置相似度阈值
threshold = 0.9

# 是否删除数字字符
delete_digits=False

# 是否对词干处理
process_stemmer=True

# 连接mongodb配置
connection_string = "mongodb://voiladata-readonly:PhieV1lu@172.31.141.244:30017/voila_gold"

# mongodb表名
spu_table_name = 'spu'
sku_table_name = 'sku'

# 基本上所有文本相似算法对反义词的识别都不是很好
unwant_antonym_array = [['long', 'short']]

# 干扰词
unwant_confusion_word = ['dress', 'mini', 'midi', 'coats', 'coat', '&', 'jackets', 'jacket']

# 品牌名词校正数据路径
brand_path = 'data/standardBrandName_old.csv'

# 常用颜色
colors = ['red', 'blue', 'yellow', 'green', 'white', 'black', 'pink', 'purple', 'orange', 'brown', 'grey']



# 日志配置
LOGGING_CONF = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '%(levelname)s|%(asctime)s|%(module)s|%(funcName)s|%(message)s'
        },
        'middle': {
            'format': '%(levelname)s|%(asctime)s|%(message)s'
        },
        'simple': {
            'format': '%(levelname)s|%(message)s'
        },
    },
    'handlers': {
        'find_sku_algorithm': {
            'level': 'INFO',
            'class': 'logging.FileHandler',
            'filename': os.path.join(base_dir, 'find_sku_algorithm.log'),
            'formatter': 'verbose',
        },

    },
    'loggers': {
        'find_sku_algorithm': {
            'handlers': ['find_sku_algorithm'],
            'level': 'INFO',
            'propagate': True,
        },
    },
}

logging.config.dictConfig(LOGGING_CONF)

findSameSkuLogger = logging.getLogger('find_sku_algorithm')



# nltk词性标注标签解释
nltk_tag_type = {
    "CC":  "并列连词",          "NNS": "名词复数",        "UH": "感叹词",
    "CD":  "基数词",            "NNP": "专有名词",        "VB": "动词原型",
    "DT":  "限定符",            "NNP": "专有名词复数",    "VBD": "动词过去式",
    "EX":  "存在词",            "PDT": "前置限定词",      "VBG": "动名词或现在分词",
    "FW":  "外来词",            "POS": "所有格结尾",      "VBN": "动词过去分词",
    "IN":  "介词或从属连词",     "PRP": "人称代词",        "VBP": "非第三人称单数的现在时",
    "JJ":  "形容词",            "PRP$": "所有格代词",     "VBZ": "第三人称单数的现在时",
    "JJR": "比较级的形容词",     "RB":  "副词",            "WDT": "以wh开头的限定词",
    "JJS": "最高级的形容词",     "RBR": "副词比较级",      "WP": "以wh开头的代词",
    "LS":  "列表项标记",        "RBS": "副词最高级",      "WP$": "以wh开头的所有格代词",
    "MD":  "情态动词",          "RP":  "小品词",          "WRB": "以wh开头的副词",
    "NN":  "名词单数",          "SYM": "符号",            "TO":  "to"
}
