# -*- coding: utf-8 -*-

import pandas as pd
import pymongo
from config import connection_string
import time


def get_mongodb_data(table_name, query_sentence):
    # 连接mongodb数据库
    database = pymongo.MongoClient(connection_string).get_database()
    # 数据表
    coll = database.get_collection(table_name)

    mongodb_data = pd.DataFrame(list(coll.find(query_sentence[0], query_sentence[1])))

    # print(mongodb_data.head())

    return mongodb_data


if __name__ == '__main__':

    from multiprocessing import Pool
    pool = Pool(152)

    database = pymongo.MongoClient(connection_string).get_database()
    # 数据表
    coll = database.get_collection('spu')
    # query_sentence = [{},{"_id": 0, 'stdCateName': 1, 'stdSubCateName': 1, 'stdSubCate2Name': 1}]

    # df = pd.DataFrame(list(coll.find({"updatedUtc": {"$gte": int(time.time())-14*86400}}, {"_id": 0, 'stdCateName': 1, 'stdSubCateName': 1, 'stdSubCate2Name': 1})))

    # df = pd.DataFrame(list(coll.find({"updatedUtc": {"$gte": int(time.time()) - 14 * 86400}})))

    query_sentence = [{"stdSubCateName": "Clothing", "stdSubCate2Name": "Coats & Jackets"},
                      {"_id": 0, 'spuId': 1, 'siteId': 1, 'title': 1, 'canonicalUrl': 1, 'maxMsrp': 1, 'siteName': 1,
                       'stdCateName': 1, 'stdSubCateName': 1, 'stdSubCate2Name': 1, 'brandName': 1, 'updatedUtc': 1}]

    query_category = [{"stdSubCateName": "Clothing", "stdSubCate2Name": "Coats & Jackets"},
                      {"_id": 0, 'stdCateName': 1, 'stdSubCateName': 1, 'stdSubCate2Name': 1}]

    df = pd.DataFrame(list(coll.find({}, {"_id": 0, 'stdCateName': 1, 'stdSubCateName': 1, 'stdSubCate2Name': 1})))

    print(df.head())
    df = df.drop_duplicates()
    df = df[df.stdCateName.notnull()]
    df = df[df.stdSubCateName.notnull()]
    df = df[df.stdSubCate2Name.notnull()]

    query_sql_list = list()
    for i in range(df.shape[0]):
        stdCateName = df.iloc[i, 0]
        stdSubCateName = df.iloc[i, 1]
        stdSubCate2Name = df.iloc[i, 2]

        query_sql = [{"stdCateName": stdCateName, "stdSubCateName": stdSubCateName, "stdSubCate2Name": stdSubCate2Name},
                     {"_id": 0, 'spuId': 1, 'siteId': 1, 'title': 1, 'canonicalUrl': 1, 'maxMsrp': 1, 'siteName': 1,
                      'stdCateName': 1, 'stdSubCateName': 1, 'stdSubCate2Name': 1, 'brandName': 1, 'updatedUtc': 1}]

        query_sql_list.append(query_sql)

    df.reset_index()
















