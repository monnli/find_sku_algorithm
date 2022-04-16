# -*- coding: utf-8 -*-

from multiprocessing import Pool
from model.tf_idf import CalculateSimi
import pymongo
from config import *
from read_write_mysql import write_to_mysql
from spu_map_sku import spu_map_sku
import pandas as pd


def multiprocessing_calculate(batch, sub_query_sql_list):
    query_sql = " "  # 便于报错时的异常输出
    try:
        cs = CalculateSimi()
        pool = Pool(15)
        result_list = list()

        for query_sql in sub_query_sql_list:
            result = pool.apply_async(cs.three_cate_simi, (query_sql,))
            result_list.append(result)
        pool.close()
        pool.join()
        count = 0
        for rs in result_list:
            df = spu_map_sku(rs.get())
            df.to_csv(f'result_{count}.csv') # 由于此步计算大，避免下游任务出错中断程序时，能快速恢复结果
            write_to_mysql(df)
            count += 1
            findSameSkuLogger.info(f"successfully write to tibd: |batch:{batch} | time:{count}")

        findSameSkuLogger.info(f'batch: {batch} was successfully finished task !!!')
    except Exception as e:
        findSameSkuLogger.info(f"batch: {batch} || query_sql: {query_sql}")
        findSameSkuLogger.info(f"exception: {e}")


def main():
    database = pymongo.MongoClient(connection_string).get_database()
    # 数据表
    coll = database.get_collection('spu')

    df = pd.DataFrame(list(coll.find({}, {"_id": 0, 'stdCateName': 1, 'stdSubCateName': 1, 'stdSubCate2Name': 1})))
    findSameSkuLogger.info(df.head())
    df = df.drop_duplicates()
    df = df[df.stdCateName.notnull()]
    df = df[df.stdSubCateName.notnull()]
    df = df[df.stdSubCate2Name.notnull()]

    findSameSkuLogger.info("******************start multiprocessing****************")

    query_sql_list = list()
    for i in range(1):
        one_name = df.iloc[i, 0]
        two_name = df.iloc[i, 1]
        three_name = df.iloc[i, 2]

        query_sql = [{"stdCateName": one_name, "stdSubCateName": two_name, "stdSubCate2Name": three_name},
                     {"_id": 0, 'spuId': 1, 'siteId': 1, 'title': 1, 'canonicalUrl': 1, 'maxMsrp': 1, 'siteName': 1,
                      'stdCateName': 1, 'stdSubCateName': 1, 'stdSubCate2Name': 1, 'brandName': 1, 'updatedUtc': 1}]
        query_sql_list.append(query_sql)

    length = df.shape[0]
    integer = length // 15 # 代表15个进程

    three_category_num = 0
    for batch in range(integer):
        three_category_num += 15
        start_index = batch * 15
        if batch == integer - 1:
            sub_query_sql_list = query_sql_list[three_category_num:]
        else:
            sub_query_sql_list = query_sql_list[start_index:sum]
        multiprocessing_calculate(batch, sub_query_sql_list)

    findSameSkuLogger.info('all batches was successfully finished task !!!')


if __name__ == '__main__':
    main()



    # cs = CalculateSimi()
    # pool = Pool(15)
    # result_list = list()
    # if debug:
    #     epoch = 2
    # else:
    #     epoch = df.shape[0]
    # for i in range(epoch):
    #     stdCateName = df.iloc[i, 0]
    #     stdSubCateName = df.iloc[i, 1]
    #     stdSubCate2Name = df.iloc[i, 2]
    #     query_sql = [{"stdCateName": stdCateName, "stdSubCateName": stdSubCateName,
    #                   "stdSubCate2Name": stdSubCate2Name},
    #                  {"_id": 0, 'spuId': 1, 'title': 1, 'canonicalUrl': 1, 'maxMsrp': 1, 'siteName': 1,
    #                   'stdCateName': 1, 'stdSubCateName': 1, 'stdSubCate2Name': 1, 'brandName': 1, 'updatedUtc': 1}]
    #     result = pool.apply_async(cs.three_cate_simi, (query_sql,))
    #     result_list.append(result)
    #     findSameSkuLogger.info(f"{i} || {stdCateName} || {stdSubCateName} || {stdSubCate2Name}")
    # pool.close()
    # pool.join()
    # count = 0
    # for rs in result_list:
    #     df = spu_map_sku(rs.get())
    #     df.to_csv('')
    #     write_to_mysql(df)
    #     count += 1
    #     findSameSkuLogger.info(f"successfully write to tibd: {count}")
    # findSameSkuLogger.info('successfully finish task !!!')

