# -*- coding: utf-8 -*-
import numpy as np
import pymongo
from read_write_mysql import write_to_mysql
from config import *
from config import sku_table_name


def match_sku(df, result_dict, match_type):
    """
    :param df:
    :param result_dict:
    :param match_type: ["hard_match", "soft_match"]
    :return:
    """

    if df.shape[0] < 1:
        return df, result_dict

    database = pymongo.MongoClient(connection_string).get_database()
    coll = database.get_collection(sku_table_name)

    if match_type == "soft_match":
        df = df[df['success'] != 1]
    elif match_type == "hard_match":
        df['success'] = np.array([0 for _ in range(df.shape[0])])
    else:
        pass
    df.index = np.array(range(df.shape[0]))

    for i in range(df.shape[0]):
        spu_id_1 = df.loc[i, "spuId"]
        spu_id_2 = df.loc[i, "spuId2"]
        spu_brand_name = df.loc[i, 'brandName']
        query_sql = [{"spuId": {"$in": [spu_id_1, spu_id_2]}},
                     {"_id": 0, 'spuId': 1, 'skuId': 1, 'canonicalUrl': 1, 'specs': 1}]
        spu_to_sku_df = pd.DataFrame(list(coll.find(query_sql[0], query_sql[1])))

        try:
            if spu_to_sku_df.shape[0] > 1:
                sub_df1 = spu_to_sku_df[spu_to_sku_df['spuId'] == spu_id_1]
                sub_df1 = sub_df1[['skuId', 'canonicalUrl', 'specs']]
                sub_df2 = spu_to_sku_df[spu_to_sku_df['spuId'] == spu_id_2]
                sub_df2 = sub_df2[['skuId', 'canonicalUrl', 'specs']]

                sub_df1.index = np.array(range(sub_df1.shape[0]))
                sub_df2.index = np.array(range(sub_df2.shape[0]))

                for j in range(sub_df1.shape[0]):
                    sku1 = sub_df1.loc[j, 'skuId']
                    specs1 = sub_df1.loc[j, 'specs']
                    name_list1 = list()
                    color_name1 = ''
                    for g in specs1:
                        name1 = (str(g['name']).lower()).strip()
                        name_list1.append(name1)
                        for c1 in [str(cc1).strip() for cc1 in name1.split(' ')]:
                            if c1 in colors:
                                color_name1 = c1
                                break
                    for k in range(sub_df2.shape[0]):
                        sku2 = sub_df2.loc[k, "skuId"]
                        specs2 = sub_df2.loc[k, "specs"]
                        name_list2 = list()
                        color_name2 = ''
                        for h in specs2:
                            name2 = (str(h['name']).lower()).strip()
                            name_list2.append(name2)
                            for c2 in [str(cc2).strip() for cc2 in name2.split(' ')]:
                                if c2 in colors:
                                    color_name2 = c2
                                    break
                        # 颜色和尺码匹配其一即成功，注意：比如化妆品这一类的specs不是颜色和尺码后续再处理TODO
                        no_color_match = 0
                        # 颜色包含关系匹配
                        color_name1_success = 0
                        color_name2_success = 0
                        if match_type == 'hard_match':
                            if (len(name_list1) == 2) and (len(name_list2) == 2):
                                if (name_list1[0] in name_list2) and (name_list1[1] in name_list2):
                                    no_color_match = 1
                                    df.loc[i, 'success'] = 1
                        elif match_type == 'soft_match':
                            if (len(name_list1) >= 1) and (len(name_list2) >= 1):
                                if len(name_list1) == 2:
                                    if (name_list1[0] in name_list2) or (name_list1[1] in name_list2):
                                        # 暂时无法区分哪个是颜色，哪个是尺码，先不考虑颜色优先的事情 TODO
                                        no_color_match = 1
                                elif len(name_list1) == 1:
                                    if name_list1[0] in name_list2:
                                        no_color_match = 1
                                else:
                                    pass

                                if no_color_match == 0:
                                    if color_name1 != '':
                                        for elem in name_list2:
                                            if color_name1 in elem:
                                                color_name1_success = 1
                                    if color_name1_success == 0:
                                        if color_name2 != '':
                                            for elem in name_list1:
                                                if color_name2 in elem:
                                                    color_name2_success = 1
                        else:
                            pass

                        if (no_color_match == 1) or (color_name1_success == 1) or (color_name2_success == 1):
                            result_dict['query_id'].append(sku1)
                            result_dict['result_id'].append(sku2)
                            result_dict['weighted_score'].append(df.loc[i, "score"])
                            result_dict['title_score'].append(df.loc[i, "tfidf_simi_score"])
                            result_dict['query_url'].append(sub_df1.loc[j, "canonicalUrl"])
                            result_dict['result_url'].append(sub_df2.loc[k, "canonicalUrl"])
                            result_dict['stdCateName'].append(df.loc[i, "stdCateName"])
                            result_dict['stdSubCateName'].append(df.loc[i, "stdSubCateName"])
                            result_dict['stdSubCate2Name'].append(df.loc[i, "stdSubCate2Name"])
                            break
            else:
                continue
        except Exception as e:
            findSameSkuLogger.info(e)

    return df, result_dict


def spu_map_sku(df):
    result_dict = {
        "query_id": list(), "result_id": list(), "weighted_score": list(), "title_score": list(), "query_url": list(),
        "result_url": list(), "stdCateName": list(), "stdSubCateName": list(), "stdSubCate2Name": list()
    }

    df, result_dict = match_sku(df, result_dict, "hard_match")
    df, result_dict = match_sku(df, result_dict, "soft_match")

    df = pd.DataFrame(result_dict)
    col_name = ["query_id", "result_id", "weighted_score", "title_score", "query_url",
                "result_url", "stdCateName", "stdSubCateName", "stdSubCate2Name"]
    df = df[col_name]

    return df



if __name__ == '__main__':
    df = pd.read_excel("C:\\Users\\29678\Desktop\\voila_china\\find_sku_algorithm\\model\\simi_result_tfidf_simi_price_Coats_Jackets.xlsx")
    df = spu_map_sku(df)
    df.to_excel("sku_match_Coats_Jackets.xlsx")




