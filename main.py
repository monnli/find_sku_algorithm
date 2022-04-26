# -*- coding: utf-8 -*-

from multiprocessing import Pool
from model.tf_idf import CalculateSimi
import pymongo
from config import *
from read_write_mysql import write_to_mysql, query_data
from data_process.preprocess_data import adjust_brand_name
from spu_map_sku import spu_map_sku
import pandas as pd
import time


def multiprocessing_calculate(batch, sub_query_sql_list):
    findSameSkuLogger.info(f'==============batch: {batch} start =============')
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
            if debug:
                df.to_csv(f'result_{batch}_{count}.csv')
            else:
                df.to_csv(f'/data/limeng/sim_result/result_{batch}_{count}.csv') # 由于此步计算大，避免下游任务出错中断程序时，能快速恢复结果
                write_to_mysql(df)
            count += 1
            findSameSkuLogger.info(f"successfully write to tibd: |batch:{batch} | time:{count}")

        findSameSkuLogger.info(f'batch: {batch} was successfully finished task !!!')
    except Exception as e:
        findSameSkuLogger.info(f"batch: {batch} || query_sql: {query_sql}")
        findSameSkuLogger.info(f"exception: {e}")


def main():
    # 清空备份表，稍后写入备份表，最后把备份表修改为主表——完成表的迅速切换，不影响线上业务
    query_data("truncate table voila_similarity_table_bak")

    database = pymongo.MongoClient(connection_string).get_database()
    # 数据表
    coll = database.get_collection('spu')

    df = pd.DataFrame(list(coll.find({}, {"_id": 0, 'siteId': 1, 'stdCateName': 1, 'brandName': 1, })))
    findSameSkuLogger.info(df.head())
    df = df[df.brandName.notnull()]

    useful_brand = list()
    for k, v in new_mapping_old_brandName.items():
        sub_df = df[df.brandName.isin(v)]
        if sub_df.shape[0] > 1:
            if sub_df.siteId.unique().shape[0] > 1:
                useful_brand.append(v)

    findSameSkuLogger.info("******************start multiprocessing****************")
    if debug:
        length = 2
    else:
        length = len(useful_brand)
    query_sql_list = list()
    for i in range(length):
        brandName = useful_brand[i]
        query_sql = [{"brandName": {"$in": brandName}},
                     {"_id": 0, 'spuId': 1, 'siteId': 1, 'title': 1, 'canonicalUrl': 1, 'maxMsrp': 1, 'siteName': 1,
                      'stdCateName': 1, 'stdSubCateName': 1, 'stdSubCate2Name': 1, 'brandName': 1, 'updatedUtc': 1}]
        query_sql_list.append(query_sql)

    integer = length // 15 # 代表15个进程
    if length > integer * 15:
        integer += 1

    three_category_num = 0
    for batch in range(integer):
        three_category_num += 15
        start_index = batch * 15
        if three_category_num > length:
            three_category_num -= 15
            sub_query_sql_list = query_sql_list[three_category_num:]
        else:
            sub_query_sql_list = query_sql_list[start_index:three_category_num]
        multiprocessing_calculate(batch, sub_query_sql_list)

    findSameSkuLogger.info('all batches was successfully finished task !!!')

    findSameSkuLogger.info('*******start to change xxx_table_bak into xxx_table*******')
    # the first, to change the xxx_table into the xxx_table_bak2
    query_data("alter table voila_similarity_table rename to voila_similarity_table_bak2")
    # the second, to change the xxx_table_bak into the xxx_table
    query_data("alter table voila_similarity_table_bak rename to voila_similarity_table")
    # finally, to change the xxx_table_bak2 into the xxx_table_bak
    query_data("alter table voila_similarity_table_bak2 rename to voila_similarity_table_bak")


if __name__ == '__main__':
    start_time = time.time()
    main()
    findSameSkuLogger.info(f"usefull total time: {(time.time() - start_time)/3600} hours")
