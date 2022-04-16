# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
import pymongo
import pymysql
from read_write_mysql import write_to_mysql
from config import *
from config import sku_table_name



def spu_map_sku(df):

    database = pymongo.MongoClient(connection_string).get_database()
    # 数据表
    coll = database.get_collection(sku_table_name)

    df.index = np.array(range(df.shape[0]))

    result_dict = {
        "query_id": list(), "result_id": list(), "weighted_score": list(), "title_score": list(), "query_url": list(),
        "result_url": list(), "stdCateName": list(), "stdSubCateName": list(), "stdSubCate2Name": list()
    }

    for i in range(df.shape[0]):
        spu_id_1 = df.loc[i, "spuId"]
        spu_id_2 = df.loc[i, "spuId2"]

        query_sql = [{"spuId": {"$in": [spu_id_1, spu_id_2]}},
                     {"_id": 0, 'spuId': 1, 'skuId': 1, 'canonicalUrl': 1, 'specs': 1}]
        spu_to_sku_df = pd.DataFrame(list(coll.find(query_sql[0], query_sql[1])))

        try:
            if spu_to_sku_df.shape[0] > 1:
                sub_df1 = spu_to_sku_df[spu_to_sku_df['spuId'] == spu_id_1]
                sub_df1 = sub_df1[['skuId', 'canonicalUrl', 'specs']]
                sub_df2 = spu_to_sku_df[spu_to_sku_df['spuId'] == spu_id_2]
                sub_df2 = sub_df2[['skuId', 'canonicalUrl', 'specs']]

                for j in range(sub_df1.shape[0]):
                    sku1 = sub_df1.iloc[j, 0]
                    specs1 = sub_df1.iloc[j, 2]
                    name_list1 = list()
                    for g in specs1:
                        name_list1.append(str(g['name']).lower())

                    for k in range(sub_df2.shape[0]):
                        sku2 = sub_df2.iloc[k, 0]
                        specs2 = sub_df2.iloc[k, 2]
                        name_list2 = list()
                        for h in specs2:
                            name_list2.append(str(h['name']).lower())
                        if (len(name_list1) == 2) and (len(name_list2) == 2):
                            if (name_list1[0] in name_list2) and (name_list1[1] in name_list2):
                                result_dict['query_id'].append(sku1)
                                result_dict['result_id'].append(sku2)
                                result_dict['weighted_score'].append(df.loc[i, "score"])
                                result_dict['title_score'].append(df.loc[i, "tfidf_simi_score"])
                                result_dict['query_url'].append(sub_df1.iloc[j, 1])
                                result_dict['result_url'].append(sub_df2.iloc[k, 1])
                                result_dict['stdCateName'].append(df.loc[i, "stdCateName"])
                                result_dict['stdSubCateName'].append(df.loc[i, "stdSubCateName"])
                                result_dict['stdSubCate2Name'].append(df.loc[i, "stdSubCate2Name"])
            else:
                continue
        except Exception as e:
            findSameSkuLogger(e)

    df = pd.DataFrame(result_dict)
    col_name = ["query_id", "result_id", "weighted_score", "title_score", "query_url",
                "result_url", "stdCateName", "stdSubCateName", "stdSubCate2Name"]
    df = df[col_name]

    return df


if __name__ == '__main__':
    df = pd.read_excel("spu_map_sku_result.xlsx")
    # df = spu_map_sku(df)
    write_to_mysql(df, 'append')


