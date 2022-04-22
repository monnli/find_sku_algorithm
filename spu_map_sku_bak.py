# -*- coding: utf-8 -*-
import numpy as np
import pymongo
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
                    color_name1 = ''
                    for g in specs1:
                        name1 = (str(g['name']).lower()).strip()
                        name_list1.append(name1)
                        if name1 in colors:
                            color_name1 = name1

                    for k in range(sub_df2.shape[0]):
                        sku2 = sub_df2.iloc[k, 0]
                        specs2 = sub_df2.iloc[k, 2]
                        name_list2 = list()
                        color_name2 = ''
                        for h in specs2:
                            name2 = (str(h['name']).lower()).strip()
                            name_list2.append(name2)
                            if name2 in colors:
                                color_name2 = name2
                        match_success = 0
                        # 颜色和尺码匹配其一即成功，注意：比如化妆品这一类的specs不是颜色和尺码后续再处理TODO
                        # 颜色包含关系匹配：颜色具有包含关系的情况也算匹配成功，比如：NFT black 包含 black
                        if (len(name_list1) >= 1) and (len(name_list2) >= 1):
                            if len(name_list1) == 2:
                                if (name_list1[0] in name_list2) or (name_list1[1] in name_list2):
                                    match_success = 1
                            elif len(name_list1) == 1:
                                if name_list1[0] in name_list2:
                                    match_success = 1
                            else:
                                # 颜色包含关系匹配
                                color_name1_success = 0
                                if color_name1 != '':
                                    for elem in name_list2:
                                        if color_name1 in elem:
                                            match_success = 1
                                            color_name1_success = 1
                                            break
                                if color_name1_success == 0 and color_name2 != '':
                                    for elem in name_list1:
                                        if color_name2 in elem:
                                            match_success = 1
                                            break
                        if match_success == 1:
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
            findSameSkuLogger.info(e)

    df = pd.DataFrame(result_dict)
    col_name = ["query_id", "result_id", "weighted_score", "title_score", "query_url",
                "result_url", "stdCateName", "stdSubCateName", "stdSubCate2Name"]
    df = df[col_name]

    return df


# if __name__ == '__main__':
#     findSameSkuLogger.info('hallo word')
#     df = pd.read_excel("model/simi_result_tfidf_simi_price_p9_14_55_66.xlsx")
#     df = spu_map_sku(df)
#     write_to_mysql(df)




