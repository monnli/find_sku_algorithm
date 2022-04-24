# -*- coding: utf-8 -*-

from gensim import corpora, models, similarities

from data_process.get_database_data import get_mongodb_data
from data_process.preprocess_data import process_title_data
import time
from config import *

import pandas as pd
import numpy as np
import json
import copy
import datetime as dt


def train_tfidf(df):
    texts = df.title.values.tolist()
    # texts = [nltk.word_tokenize(i) for i in texts]
    dictionary = corpora.Dictionary(texts, prune_at=2000000)
    corpus = [dictionary.doc2bow(document) for document in texts]
    tfidf_model = models.TfidfModel(corpus)
    
    return tfidf_model, dictionary


def tfidf_cosin_simi(sub_df, tfidf_model, dictionary, query_text):
    try:
        sub_corpus = [dictionary.doc2bow(document) for document in sub_df.title.tolist()]
        query_text = dictionary.doc2bow(query_text)
        sub_corpus.append(query_text)
        corpus_tfidf = tfidf_model[sub_corpus]
        tfidf_index = similarities.MatrixSimilarity(corpus_tfidf, num_best=None)

        sims = tfidf_index[query_text]
    except Exception as e:
        findSameSkuLogger.info(e)
        findSameSkuLogger.info(f"query_text: {query_text}")
        sims = [0 for _ in range(sub_df.shape[0])]
    
    return sims


class CalculateSimi(object):
    def __init__(self, ):
        pass

    def three_cate_simi(self, query_sentence):
        # 计算14天前的节点日期date_point
        findSameSkuLogger.info(f"query_sentence: {query_sentence}")
        date_point = pd.datetime.datetime.now().date() - dt.timedelta(recent_ndays)

        product_data = get_mongodb_data(spu_table_name, query_sentence)

        product_data, product_full_data = process_title_data(product_data, normal_brand_names)
        tfidf_model, dictionary = train_tfidf(product_data)

        df = copy.deepcopy(product_full_data)
        del product_full_data
        del product_data

        df['updatedUtc'] = df.apply(lambda x: pd.to_datetime(int(x["updatedUtc"]), unit='s'), axis=1)
        df['updatedUtc'] = df.apply(lambda x: (x["updatedUtc"]).date(), axis=1)

        # 添加是否计算过相似的标记，初始为0，计算过更新为1
        df['is_cal_tag'] = np.array([0 for i in range(df.shape[0])])
        # 更新行索引便于准确定位行位置，更新相关值
        df.index = np.array(range(df.shape[0]))
        # 添加score列便于更新最大相似分值
        df['score'] = np.array([0 for i in range(df.shape[0])])
        # 增加2分分值列，便于数据分析，线上不用
        df['price_score'] = np.array([0 for i in range(df.shape[0])])
        df['maxMsrp2'] = np.array([0 for i in range(df.shape[0])])
        df['tfidf_simi_score'] = np.array([0 for i in range(df.shape[0])])

        # 添加一列spuid用于存储找到的最相似的商品的spuid
        df['spuId2'] = np.array(['' for i in range(df.shape[0])])

        # 添加被查到的标题和网址，用于分析数据，线上就不用了。
        df['raw_title2'] = np.array(['' for i in range(df.shape[0])])
        df['title2'] = np.array(['' for i in range(df.shape[0])])
        df['canonicalUrl2'] = np.array(['' for i in range(df.shape[0])])

        df['siteName2'] = np.array(['' for i in range(df.shape[0])])

        start_time = time.time()
        length = df.shape[0]
        for row_index in range(length):
            siteName = df.loc[row_index, 'siteName']
            siteId = df.loc[row_index, 'siteId']
            brandName = df.loc[row_index, 'brandName']
            # stdCateName = df.loc[row_index, 'stdCateName']
            # stdSubCateName = df.loc[row_index, 'stdSubCateName']
            maxMsrp = df.loc[row_index, 'maxMsrp']
            title_text = df.loc[row_index, 'title']

            if title_text == None or title_text == '':
                df.drop(row_index, inplace=True)
                continue

            # 过滤条件
            sub_df = df[df['siteId'] != siteId]
            sub_df = sub_df[sub_df['brandName'] == brandName]
            sub_df = sub_df[sub_df['updatedUtc'] >= date_point]
            sub_df = sub_df[sub_df['is_cal_tag'] != 1]
            try:
                sub_df['price_score'] = sub_df.apply(lambda x: min(maxMsrp, x['maxMsrp']) / max(maxMsrp, x['maxMsrp']),
                                                     axis=1)
                sub_df = sub_df[sub_df['price_score'] >= 0.9]
            except Exception as e:
                df.drop(row_index, inplace=True)
                findSameSkuLogger.info(f"row_index: {row_index}")
                continue

            # # 过滤条件
            # sub_df = df[df['stdCateName'] == stdCateName]
            # sub_df = sub_df[sub_df['stdSubCateName'] == stdSubCateName]
            # sub_df = sub_df[sub_df['siteId'] != siteId]
            # sub_df = sub_df[sub_df['brandName'] == brandName]
            # sub_df = sub_df[sub_df['updatedUtc'] >= date_point]
            # sub_df = sub_df[sub_df['is_cal_tag'] != 1]

            if sub_df.shape[0] == 0:
                df.drop(row_index, inplace=True)
            else:
                try:
                    # sub_df['title_0'] = np.array([title_text for i in range(sub_df.shape[0])])
                    sims = tfidf_cosin_simi(sub_df, tfidf_model, dictionary, title_text)
                    sims = sims[0:-1] # 最后一个是title_text与自己的相似度要去掉
                    sub_df_index = sub_df.index.tolist()
                    index_score_list = zip(sub_df_index, sims)

                    max_score = -float('inf')
                    price_score = -float('inf')
                    max_score_row_index = 0
                    maxMsrp2 = 0
                    tfidf_simi_score = 0
                    siteName2 = ''
                    for k, v in index_score_list:
                        p2 = sub_df.loc[k, 'maxMsrp']
                        p_score = min(maxMsrp, p2) / max(maxMsrp, p2)
                        score = 0.5 * p_score + 0.5 * v

                        if score > max_score:
                            max_score = score
                            price_score = p_score
                            max_score_row_index = k
                            maxMsrp2 = p2
                            tfidf_simi_score = v
                            siteName2 = sub_df.loc[k, 'siteName']

                    if (max_score >= threshold) and (price_score >= 0.9) and (tfidf_simi_score >= 0.9):
                        df.loc[row_index, 'score'] = max_score
                        df.loc[row_index, 'is_cal_tag'] = 1
                        sub_df = sub_df.loc[max_score_row_index, ['spuId', 'raw_title', 'title', 'canonicalUrl']]
                        spuId2 = sub_df['spuId']
                        df.loc[row_index, 'spuId2'] = spuId2
                        raw_title2 = sub_df['raw_title']
                        df.loc[row_index, 'raw_title2'] = raw_title2
                        title2 = sub_df['title']
                        df.loc[row_index, 'title2'] = json.dumps(title2)
                        canonicalUrl2 = sub_df['canonicalUrl']
                        df.loc[row_index, 'canonicalUrl2'] = canonicalUrl2

                        df.loc[row_index, 'price_score'] = price_score
                        df.loc[row_index, 'maxMsrp2'] = maxMsrp2
                        df.loc[row_index, 'tfidf_simi_score'] = tfidf_simi_score
                        df.loc[row_index, 'siteName2'] = siteName2

                        findSameSkuLogger.info(f"max_score_row_index: {max_score_row_index}||max_score: {max_score}")
                    else:
                        df.drop(row_index, inplace=True)
                except Exception as e:
                    df.drop(row_index, inplace=True)
                    findSameSkuLogger.info('exception handling:', e)
            findSameSkuLogger.info(f"跑了{row_index + 1}次 || 还有{length - row_index - 1}次没有跑 || 总耗时:{(time.time() - start_time) / 60}分钟")

        return df


if __name__ == '__main__':

    # query_sentence = [{"stdSubCateName": "Clothing", "stdSubCate2Name": "Coats & Jackets"},
    #                   {"_id": 0, 'spuId': 1, 'siteId': 1, 'title': 1, 'canonicalUrl': 1, 'maxMsrp': 1, 'siteName': 1,
    #                    'stdCateName': 1, 'stdSubCateName': 1, 'stdSubCate2Name': 1, 'brandName': 1, 'updatedUtc': 1}]
    #  Coats & Jackets
    query_sentence = [{"stdSubCateName": "Clothing", "stdSubCate2Name": "Pants"},
                      {"_id": 0, 'spuId': 1, 'siteId': 1, 'title': 1, 'canonicalUrl': 1, 'maxMsrp': 1, 'siteName': 1,
                       'stdCateName': 1, 'stdSubCateName': 1, 'stdSubCate2Name': 1, 'brandName': 1, 'updatedUtc': 1}]
    cs = CalculateSimi()
    df = cs.three_cate_simi(query_sentence)
    df.to_excel("simi_result_tfidf_simi_price_study_filter_old.xlsx")






