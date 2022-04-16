# -*- coding: utf-8 -*-
from data_process.get_database_data import get_mongodb_data
from data_process.preprocess_data_bak import process_title_data
import time
from config import *

import pandas as pd
import numpy as np
import json
import copy


def get_jaccard_simi(title_text, sub_df):
    title_list = sub_df['title'].values.tolist()
    title_list = [set(i) for i in title_list]
    title_text = set(title_text)
    jaccard_score_list = [len(title_text & i) / len(title_text | i) if len(title_text | i) != 0 else 0 for i in title_list]
    return jaccard_score_list


class CalculateJaccardSimi(object):
    def __init__(self):
        pass
    brand_names = pd.read_csv("data_process/standardBrandName.csv", encoding='utf-8-sig')
    query_sentence = [{"stdSubCateName": "Clothing", "stdSubCate2Name": "Coats & Jackets"},
                      {"_id": 0, 'spuId': 1, 'title': 1, 'canonicalUrl': 1, 'maxMsrp': 1,
                       'siteName': 1, 'stdCateName': 1, 'stdSubCateName': 1, 'stdSubCate2Name': 1, 'brandName': 1}]
    df = get_mongodb_data(spu_table_name, query_sentence)
    df = product_full_data = process_title_data(df, brand_names)

    df.index = np.array(range(df.shape[0]))
    result_digit_cols = ['is_cal_tag', 'score', 'price_score', 'maxMsrp2', 'jaccard_score']
    for col_name in result_digit_cols:
        df[col_name] = np.array([0 for _ in range(df.shape[0])])

    result_str_cols = ["spuId2", "raw_title2", "title2", "canonicalUrl2"]
    for name in result_str_cols:
        df[name] = np.array(['' for y in range(df.shape[0])])

    start_time = time.time()
    length = df.shape[0]
    for row_index in range(length):
        siteName = df.loc[row_index, 'siteName']
        brandName = df.loc[row_index, 'brandName']
        stdCateName = df.loc[row_index, 'stdCateName']
        stdSubCateName = df.loc[row_index, 'stdSubCateName']
        title_text = df.loc[row_index, 'title']
        maxMsrp = df.loc[row_index, 'maxMsrp']

        sub_df = df[df['stdCateName'] == stdCateName]
        sub_df = sub_df[sub_df['stdSubCateName'] == stdSubCateName]
        sub_df = sub_df[sub_df['siteName'] != siteName]
        sub_df = sub_df[sub_df['brandName'] == brandName]
        sub_df = sub_df[sub_df['is_cal_tag'] != 1]

        if sub_df.shape[0] == 0:
            df.drop(row_index, inplace=True)
        else:
            sub_df['title_0'] = np.array(title_text for _ in range(sub_df.shape[0]))
            jaccard_score_list = get_jaccard_simi(title_text, sub_df)
            sub_df_index = sub_df.index.tolist()
            index_score_list = zip(sub_df_index, jaccard_score_list)

            max_score = -float('inf')
            price_score = -float('inf')
            max_score_row_index = 0
            maxMsrp2 = 0
            jaccard_score = 0
            for k, v in index_score_list:
                p2 = sub_df.loc[k, 'maxMsrp']
                p_score = min(maxMsrp, p2) / max(maxMsrp, p2)
                score = 0.5 * p_score + 0.5 * v

                if score > max_score:
                    max_score = score
                    price_score = p_score
                    max_score_row_index = k
                    maxMsrp2 = p2
                    jaccard_score = v

            if (max_score >= threshold) and (price_score >= 0.6) and (jaccard_score >= 0.6):
                df.loc[row_index, 'score'] = max_score
                df.loc[row_index, 'is_cal_tag'] = 1
                sub_df = sub_df.loc[max_score_row_index, ['spuId', 'raw_title', 'title', 'canonicalUrl']]

                dic = {"spuId": "spuId2", "raw_title": "raw_title2", "title": "title2", "canonicalUrl": "canonicalUrl2"}
                for k, value in dic.items():
                    if k == 'title':
                        df.loc[row_index, value] = json.dumps(sub_df[k])
                    else:
                        df.loc[row_index, value] = sub_df[k]

                df.loc[row_index, 'price_score'] = price_score
                df.loc[row_index, 'maxMsrp2'] = maxMsrp2
                df.loc[row_index, 'jaccard_score'] = jaccard_score

                print(f"max_score_row_index: {max_score_row_index}||max_score: {max_score}")
            else:
                df.drop(row_index, inplace=True)

        print(f"跑了{row_index + 1}次 || 还有{length - row_index - 1}次没有跑 || 总耗时:{(time.time() - start_time) / 60}分钟")

    # # 入库字段统一
    # df = df.rename(columns={"spuId": "query_id", "spuId2":"result_id", "score": "weighted_score",
    #                         "jaccard_score": "title_score", "canonicalUrl": "query_url",
    #                         "canonicalUrl2": "result_url"})
    #
    # write_base_cols = ["query_id", "result_id", "weighted_score", "title_score", "query_url", "result_url",
    #                    "stdCateName2", "stdSubCateName2", "stdSubCate2Name2"]
    # # # 入库字段
    # # df = df[write_base_cols]
    # # df = df.rename(columns={"stdCateName2": "stdCateName", "stdSubCateName2":"stdSubCateName",
    # #                         "stdSubCate2Name2": "stdSubCate2Name"})

    df.to_excel("simi_result_jaccard_price_55_66.xlsx")

    # return df


if __name__ == '__main__':
    cjs = CalculateJaccardSimi()


