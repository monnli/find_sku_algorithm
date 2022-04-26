# -*- coding: utf-8 -*-

from sqlalchemy import create_engine
import pymysql
from datetime import datetime
import numpy as np

from config import debug, findSameSkuLogger


def write_to_mysql(df):
    """ write_type: 'append' """
    # now_date = datetime.now().date()
    now_date = datetime.now()

    engine = create_engine("mysql+pymysql://similarity:ye6Iep8S@172.31.141.244:31545/voila_similarity?charset=utf8")

    df['create_time'] = np.array([now_date for _ in range(df.shape[0])])
    df['update_time'] = np.array([now_date for _ in range(df.shape[0])])
    try:
        if debug:
            df.to_sql(name='voila_similarity_table_test', con=engine, if_exists="append", index=False)
        else:
            df.to_sql(name='voila_similarity_table_bak', con=engine, if_exists='append', index=False)
    except Exception as e:
        findSameSkuLogger.info(f"write_to_mysql exception: {e}")


def query_data(sql):
    connection = pymysql.connect(
        host='172.31.141.244',
        port=31545,
        user='similarity',
        password='ye6Iep8S',
        db='voila_similarity'
    )
    try:
        with connection.cursor() as cursor:
            count = cursor.execute(sql)  # 影响的行数
            print(count)
            result = cursor.fetchall()  # 取出所有行

            for i in result:  # 打印结果
                print(i)
            connection.commit()  # 提交事务
    except:
        connection.rollback()  # 若出错了，则回滚
    finally:
        cursor.close()
        connection.close()


# if __name__ == '__main__':
#     debug = False
#     import pandas as pd
#     df = pd.read_excel("C:\\Users\\29678\\Desktop\\voila_similarity_table.xls")
#     write_to_mysql(df)
