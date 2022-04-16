# -*- coding: utf-8 -*-

import pandas as pd
import pymongo
from config import connection_string
import time
from config import recent_ndays

def get_mongodb_data(table_name, query_sentence):
    # 连接mongodb数据库
    database = pymongo.MongoClient(connection_string).get_database()
    # 数据表
    coll = database.get_collection(table_name)

    mongodb_data = pd.DataFrame(list(coll.find(query_sentence[0], query_sentence[1])))

    # print(mongodb_data.head())

    return mongodb_data

# def get_mongodb_data(table_name, query_sentence, recent_ndays, is_all_data):
#     # 连接mongodb数据库
#     database = pymongo.MongoClient(connection_string).get_database()
#     # 数据表
#     coll = database.get_collection(table_name)
#     if not is_all_data:
#         query_sentence[0]["updatedUtc"] = {"$gte": int(time.time()) - recent_ndays * 86400}
#     mongodb_data = pd.DataFrame(list(coll.find(query_sentence[0], query_sentence[1])))
#
#     return mongodb_data


if __name__ == '__main__':
    table_name = 'spu'

    query_sentence = [{"stdSubCateName": "Clothing", "stdSubCate2Name": "Coats & Jackets"},
                      {"_id": 0, 'spuId': 1, 'siteId': 1, 'title': 1, 'canonicalUrl': 1, 'maxMsrp': 1,
                       'siteName': 1, 'stdCateName': 1, 'stdSubCateName': 1, 'stdSubCate2Name': 1,
                       'brandName': 1, 'updatedUtc': 1}]

    # df = get_mongodb_data(table_name, query_sentence, recent_ndays, True)
    # sub_df = get_mongodb_data(table_name, query_sentence, recent_ndays, False)
    #
    # print(f"df: {df.shape}---\ntmp: {sub_df.shape}----------")




