# -*- coding: utf-8 -*-

from sqlalchemy import create_engine
from datetime import datetime
import numpy as np

from config import debug


def write_to_mysql(df):
    """ write_type: 'append' """
    now_date = datetime.now().date()

    engine = create_engine("mysql+pymysql://similarity:ye6Iep8S@172.31.141.244:31545/voila_similarity?charset=utf8")

    df['create_time'] = np.array([now_date for _ in range(df.shape[0])])
    df['update_time'] = np.array([now_date for _ in range(df.shape[0])])

    if debug:
        df.to_sql(name='voila_similarity_table_test', con=engine, if_exists="append", index=False)
    else:
        df.to_sql(name='voila_similarity_table', con=engine, if_exists='replace', index=False)


if __name__ == '__main__':
    debug = False
    import pandas as pd
    df = pd.read_excel("C:\\Users\\29678\\Desktop\\voila_similarity_table.xls")
    write_to_mysql(df)
