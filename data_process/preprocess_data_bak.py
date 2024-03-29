# -*- coding: utf-8 -*-

import nltk
from nltk.stem import *
from nltk.corpus import stopwords

from config import unwant_antonym_array, unwant_confusion_word
from config import brand_path
from config import delete_digits, process_stemmer
from config import spu_table_name, sku_table_name
from config import colors
import copy


def process_text_data(text):
    """
    :param text: a title string
    :return:
    """
    # 去除标点符号
    tokenizer = nltk.RegexpTokenizer(r"\w+")
    text = tokenizer.tokenize(text.lower())

    # 去除掉无用的stop words
    text = [word for word in text if word not in stopwords.words('english')]

    # 去除掉数字(可选)
    if delete_digits:
        # text = ' '.join([i for i in text if not i.isdigit()])
        text = [i for i in text if not i.isdigit()]

    # 删除干扰词
    if len(text) != 1:
        text = [word for word in text if word not in unwant_confusion_word]

    # 词干处理(可选)
    if process_stemmer:
        stemmer = PorterStemmer()
        text = [stemmer.stem(token) for token in text]

    # 去掉颜色, 颜色用于sku时匹配
    text = [word for word in text if word not in colors]

    return text


def process_title_data(product_data, brand_names, model_type):
    """
    :param product_data:
    :param brand_names:
    :param model_type: value in ['jaccard', 'tfidf']
    :return:
    """
    product_data = product_data[product_data['stdCateName'].notnull()]
    product_data = product_data[product_data['stdSubCateName'].notnull()]
    product_data = product_data[product_data['stdSubCate2Name'].notnull()]
    product_data = product_data[product_data['siteName'].notnull()]
    product_data = product_data[product_data['brandName'].notnull()]

    # 增加原始标题便于数据分析和查找
    product_data['raw_title'] = product_data['title']
    # 读取品牌映射信息
    right_brand_name = brand_names['correct_brand_name'].to_list()
    wrong_brand_name = brand_names['wrong_brand_name'].to_list()
    brand_name_pairs = zip(right_brand_name, wrong_brand_name)

    def adjust_brand_name(raw_brand_name):
        for brand_pair in brand_name_pairs:
            if raw_brand_name in brand_pair:
                return brand_pair[0]
        return str(raw_brand_name).lower()

    # 校正品牌名词, 校正后的brandName是小写的
    product_data['brandName'] = product_data.apply(lambda x: adjust_brand_name(x['brandName']), axis=1)

    # 添加品牌名词删除，品牌名是小写，title也要变小写
    product_data['title'] = product_data.apply(lambda x: x['title'].lower(), axis=1)
    product_data['title'] = product_data.apply(lambda x: (x['title'].replace(x['brandName'], '')).strip(), axis=1)

    # 删除站点名称——找同款找的是除本站外的其他站点的商品，因此站点不同的，该删除
    # 注意：站点名称还没有校正！！！
    product_data['siteName'] = product_data.apply(lambda x: str(x['siteName']), axis=1)
    product_data['title'] = product_data.apply(lambda x: (x['title'].replace(x['siteName'], '')).strip(), axis=1)

    # 删除三级目录名
    product_data['stdSubCate2Name'] = product_data.apply(lambda x: str(x['stdSubCate2Name']), axis=1)
    product_data['title'] = product_data.apply(lambda x: (x['title'].replace(x['stdSubCate2Name'], '')).strip(), axis=1)

    # 寻找同款商品要用全量数据，
    product_full_data = copy.deepcopy(product_data)
    # 去掉重复的标题, 用于训练需要将重复预料删除
    product_data = product_data.drop_duplicates(subset='title', keep='first', inplace=False)

    # 清洗文本数据
    product_data['title'] = product_data.apply(lambda x: process_text_data(x['title']), axis=1)
    product_full_data['title'] = product_full_data.apply(lambda x: process_text_data(x['title']), axis=1)

    if model_type != 'jaccard':
        return product_data, product_full_data
    else:
        return product_full_data


if __name__ == '__main__':
    query_sentence = [{"stdSubCateName": "Clothing", "stdSubCate2Name": "Coats & Jackets"},
                      {"_id": 0, 'spuId': 1, 'title': 1, 'canonicalUrl': 1, 'maxMsrp': 1,
                       'siteName': 1, 'stdCateName': 1, 'stdSubCateName': 1, 'stdSubCate2Name': 1, 'brandName': 1}]

    process_title_data(spu_table_name, query_sentence)
