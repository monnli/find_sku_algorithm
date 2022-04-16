# -*- coding: utf-8 -*-

from multiprocessing import Pool
from model.tf_idf import CalculateSimi
import pymongo
from config import *
from read_write_mysql import write_to_mysql
from spu_map_sku import spu_map_sku


def main():
    cs = CalculateSimi()

    database = pymongo.MongoClient(connection_string).get_database()
    # 数据表
    coll = database.get_collection('spu')
    # query_sentence = [{"stdSubCateName": "Clothing", "stdSubCate2Name": "Coats & Jackets"},
    #                   {"_id": 0, 'spuId': 1, 'siteId': 1, 'title': 1, 'canonicalUrl': 1, 'maxMsrp': 1, 'siteName': 1,
    #                    'stdCateName': 1, 'stdSubCateName': 1, 'stdSubCate2Name': 1, 'brandName': 1, 'updatedUtc': 1}]
    #
    # query_category = [{"stdSubCateName": "Clothing", "stdSubCate2Name": "Coats & Jackets"},
    #                   {"_id": 0, 'stdCateName': 1, 'stdSubCateName': 1, 'stdSubCate2Name': 1}]

    df = pd.DataFrame(list(coll.find({}, {"_id": 0, 'stdCateName': 1, 'stdSubCateName': 1, 'stdSubCate2Name': 1})))
    findSameSkuLogger.info(df.head())
    df = df.drop_duplicates()
    df = df[df.stdCateName.notnull()]
    df = df[df.stdSubCateName.notnull()]
    df = df[df.stdSubCate2Name.notnull()]

    findSameSkuLogger.info("******************start multiprocessing****************")

    pool = Pool(15)
    result_list = list()

    if debug:
        epoch = 2
    else:
        epoch = 15

    for i in range(epoch):
        stdCateName = df.iloc[i, 0]
        stdSubCateName = df.iloc[i, 1]
        stdSubCate2Name = df.iloc[i, 2]

        query_sql = [{"stdCateName": stdCateName, "stdSubCateName": stdSubCateName, "stdSubCate2Name": stdSubCate2Name},
                     {"_id": 0, 'spuId': 1, 'title': 1, 'canonicalUrl': 1, 'maxMsrp': 1, 'siteName': 1,
                      'stdCateName': 1, 'stdSubCateName': 1, 'stdSubCate2Name': 1, 'brandName': 1, 'updatedUtc': 1}]
        result = pool.apply_async(cs.three_cate_simi, (query_sql,))
        result_list.append(result)
        findSameSkuLogger.info(f"{i} || {stdCateName} || {stdSubCateName} || {stdSubCate2Name}")

    pool.close()
    pool.join()
    count = 0
    for rs in result_list:
        df = spu_map_sku(rs.get())
        write_to_mysql(df)
        count += 1
        findSameSkuLogger.info(f"successfully write to tibd: {count}")

    findSameSkuLogger.info('successfully finish task !!!')

    # query_sql = ''
    # for i in range(1):
    #     stdCateName = df.iloc[i, 0]
    #     stdSubCateName = df.iloc[i, 1]
    #     stdSubCate2Name = df.iloc[i, 2]
    #
    #     query_sql = [{"stdCateName": stdCateName, "stdSubCateName": stdSubCateName, "stdSubCate2Name": stdSubCate2Name},
    #                  {"_id": 0, 'spuId': 1, 'siteId': 1, 'title': 1, 'canonicalUrl': 1, 'maxMsrp': 1, 'siteName': 1,
    #                   'stdCateName': 1, 'stdSubCateName': 1, 'stdSubCate2Name': 1, 'brandName': 1, 'updatedUtc': 1}]
    #
    # cs.three_cate_simi(query_sql)


if __name__ == '__main__':
    main()

