# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
import pymongo
from read_write_mysql import write_to_mysql
from config import *
from config import sku_table_name

import json


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
                     {"_id": 0, 'spuId': 1, 'skuId': 1, 'canonicalUrl': 1, 'specs': 1, "siteName": 1}]
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
                                    result_dict['success'].append(1)
                                    df.loc[i, 'success'] = 1
                        elif match_type == 'soft_match':
                            if (len(name_list1) >= 1) and (len(name_list2) >= 1):
                                if len(name_list1) == 2:
                                    if (name_list1[0] in name_list2) or (name_list1[1] in name_list2):
                                        # 暂时无法区分哪个是颜色，哪个是尺码，先不考虑颜色优先的事情 TODO
                                        no_color_match = 1
                                        result_dict['success'].append(2)
                                elif len(name_list1) == 1:
                                    if name_list1[0] in name_list2:
                                        no_color_match = 1
                                        result_dict['success'].append(3)
                                else:
                                    pass

                                if no_color_match == 0:
                                    if color_name1 != '':
                                        for elem in name_list2:
                                            if color_name1 in elem:
                                                color_name1_success = 1
                                                result_dict['success'].append(4)
                                    if color_name1_success == 0:
                                        if color_name2 != '':
                                            for elem in name_list1:
                                                if color_name2 in elem:
                                                    color_name2_success = 1
                                                    result_dict['success'].append(5)

                        result_dict['query_id'].append(sku1)
                        result_dict['result_id'].append(sku2)
                        result_dict['weighted_score'].append(df.loc[i, "score"])
                        result_dict['title_score'].append(df.loc[i, "tfidf_simi_score"])
                        result_dict['query_url'].append(sub_df1.loc[j, "canonicalUrl"])
                        result_dict['result_url'].append(sub_df2.loc[k, "canonicalUrl"])
                        result_dict['stdCateName'].append(df.loc[i, "stdCateName"])
                        result_dict['stdSubCateName'].append(df.loc[i, "stdSubCateName"])
                        result_dict['stdSubCate2Name'].append(df.loc[i, "stdSubCate2Name"])
                        result_dict['brandName'].append(spu_brand_name)
                        result_dict['query_specs'].append(sub_df1.loc[j, "specs"])
                        result_dict['result_specs'].append(sub_df2.loc[k, "specs"])

                        if (no_color_match == 1) or (color_name1_success == 1) or (color_name2_success == 1):
                            # result_dict['success'].append(1)
                            break
                        else:
                            result_dict['success'].append(0)
            else:
                continue
        except Exception as e:
            findSameSkuLogger.info(e)

    return df, result_dict


def spu_map_sku(df):
    # result_dict = {
    #     "query_id": list(), "result_id": list(), "weighted_score": list(), "title_score": list(), "query_url": list(),
    #     "result_url": list(), "stdCateName": list(), "stdSubCateName": list(), "stdSubCate2Name": list()
    # }

    result_dict = {
        "query_id": list(), "result_id": list(), "weighted_score": list(), "title_score": list(), "query_url": list(),
        "result_url": list(), "stdCateName": list(), "stdSubCateName": list(), "stdSubCate2Name": list(),
        "brandName": list(), "query_specs": list(), "result_specs": list(), "success": list()
    }

    df, result_dict = match_sku(df, result_dict, "hard_match")
    df, result_dict = match_sku(df, result_dict, "soft_match")

    df = pd.DataFrame(result_dict)

    # col_name = ["query_id", "result_id", "weighted_score", "title_score", "query_url",
    #             "result_url", "stdCateName", "stdSubCateName", "stdSubCate2Name"]
    # df = df[col_name]

    return df


# # 测试 specs颜色属性具有包含关系就可以匹配的数量
def contain_color_relation():
    import json
    df = pd.read_excel(
        "C:\\Users\\29678\\Desktop\\voila_china\\find_sku_algorithm\\data_process\\coat_jacket_sku_specs所有匹配对_bak.xlsx")
    tmp = df[df.success == 0]
    tmp['match'] = np.array([0 for _ in range(tmp.shape[0])])
    tmp.index = range(tmp.shape[0])
    colors = ['red', 'orange', 'yellow', 'green', 'blue', 'purple', 'gray', 'pink', 'black', 'white', 'brown']
    for i in range(tmp.shape[0]):
        try:
            # specs1的类似于："[{'a': 3}, {'b': 4}]",需要使用json.loads解析过来，但是json.loads函数需要"[{'a': 3}, {'b': 4}]"
            # 内部是双引号，如果是单引号就会报错，因此需要处理一下
            specs1 = tmp.loc[i, 'query_specs']
            specs1 = specs1.replace("'", '"')
            specs1 = json.loads(specs1)
            name = ''
            for k in specs1:
                n = k['name']
                n = n.lower().strip()
                if n in colors:
                    name = n
                    break
            if name != '':
                specs2 = tmp.loc[i, 'result_specs']
                specs2 = specs2.replace("'", '"')
                specs2 = json.loads(specs2)
                for j in specs2:
                    m = j['name']
                    m = m.lower().strip()
                    if name in m:
                        tmp.loc[i, 'match'] = 1
                        break
        except Exception as e:
            print(e)
    print(tmp.head())


# 统计品牌在各站点的分布情况
def brand_map_site():
    df = pd.read_excel("C:\\Users\\29678\\Desktop\\voila_china\\站点名类目品牌_统计.xlsx")
    brand_df = pd.read_excel("C:\\Users\\29678\\Desktop\\voila_china\\brand_name_new.xlsx")

    # 修正品牌名
    brand_df = brand_df[['raw_brand', 'right_brand']]
    brand_df = brand_df.drop_duplicates()
    brand_df.index = brand_df.raw_brand
    brand_df = brand_df['right_brand']
    brand_dict = brand_df.to_dict()

    for k in range(df.shape[0]):
        raw_name = df.loc[k, 'brandName']
        if raw_name in brand_dict:
            df.loc[k, 'brandName'] = brand_dict.get(raw_name)

    tmp = df[['siteName', 'brandName', 'sku数量']]
    tmp = tmp.groupby(['siteName', 'brandName']).sum('sku数量')
    tmp = tmp.reset_index()
    brand_list = tmp.brandName.tolist()
    brand_list = list(set(brand_list))

    brand_map_site = pd.DataFrame()
    brand_map_site['brandName'] = ''
    brand_map_site['brand_map_site_detail'] = ''
    brand_map_site['site_num'] = 0
    brand_map_site['sku_num'] = 0

    for i in range(len(brand_list)):
        name = brand_list[i]
        t = tmp[tmp.brandName == name]
        t.index = t.siteName
        t = t['sku数量']
        brand_map_site_detail = json.dumps(t.to_dict())
        site_num = t.shape[0]
        sku_num = t.sum()

        brand_map_site.loc[i, 'brandName'] = name
        brand_map_site.loc[i, 'brand_map_site_detail'] = brand_map_site_detail
        brand_map_site.loc[i, 'site_num'] = site_num
        brand_map_site.loc[i, 'sku_num'] = sku_num

    brand_map_site = brand_map_site.sort_values('sku_num', ascending=False)
    brand_map_site.to_excel("C:\\Users\\29678\\Desktop\\voila_china\\所有品牌分布到各站点的详情_统计(修正的品牌名).xlsx")

    print(brand_map_site.head())


if __name__ == '__main__':
    df = pd.read_excel("C:\\Users\\29678\Desktop\\voila_china\\find_sku_algorithm\\model\\simi_result_tfidf_simi_price_study_specs.xlsx")
    df = spu_map_sku(df)
    df.to_excel("coat_jacket_sku_specs所有匹配对_7.xlsx")









