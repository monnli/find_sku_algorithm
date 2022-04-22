# -*- coding: utf-8 -*-
import json

import pandas as pd
import numpy as np

df = pd.read_excel("C:\\Users\\29678\\Desktop\\voila_china\\find_sku_algorithm\\data_process\\coat_jacket_sku_specs所有匹配对_2.xlsx")
print(df.head())
df = df[df.success == 0]
# tmp = df[['query_id', 'result_id', 'query_siteName', 'result_siteName', 'brandName', 'query_specs', 'result_specs']]
tmp = df
tmp['query_specs'] = tmp.apply(lambda x: x['query_specs'].replace("'", '"'), axis=1)
tmp['result_specs'] = tmp.apply(lambda x: x['result_specs'].replace("'", '"'), axis=1)

tmp.index = np.array(range(tmp.shape[0]))
tmp['query_name1'] = np.array(['' for _ in range(tmp.shape[0])])
tmp['query_name2'] = np.array(['' for _ in range(tmp.shape[0])])
tmp['result_name1'] = np.array(['' for _ in range(tmp.shape[0])])
tmp['result_name2'] = np.array(['' for _ in range(tmp.shape[0])])

for i in range(tmp.shape[0]):
    try:
        query_specs = json.loads(tmp.loc[i, 'query_specs'])
        if len(query_specs) <= 1:
            tmp.loc[i, 'query_name1'] = (query_specs[0])['name']
        else:
            tmp.loc[i, 'query_name1'] = (query_specs[0])['name']
            tmp.loc[i, 'query_name2'] = (query_specs[1])['name']
    except Exception as e:
        print(e)

    try:
        query_specs = json.loads(tmp.loc[i, 'result_specs'])
        if len(query_specs) <= 1:
            tmp.loc[i, 'result_name1'] = (query_specs[0])['name']
        else:
            tmp.loc[i, 'result_name1'] = (query_specs[0])['name']
            tmp.loc[i, 'result_name2'] = (query_specs[1])['name']
    except Exception as e:
        print(e)
tmp.to_excel("color_size_study.xlsx")

print(tmp.head())