#encoding=utf-8
import pandas as pd
import pymysql
import MySQLdb
import datetime,time
import numpy as np
import pandas as pd
import copy
from datacleaner import autoclean

def store_modified_data_into_new_table(clean_data_df):
    print clean_data_df
    from sqlalchemy import create_engine

    table_name = "final_all_stocks_cleaned_factors_table"
    engine = create_engine('mysql://root:zjz4818774@localhost/invest_latest?charset=utf8')
    try:
        clean_data_df.to_sql(table_name,engine,if_exists='append');
    except Exception as e:
        #如果写入数据库失败，写入日志表，便于后续分析处理
        print e
        print "storation wrong"


def pandas_read_data_from_table():

    conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='zjz4818774', db='invest_latest')
    cursor = conn.cursor()
    # cursor.execute("DROP TABLE IF EXISTS test")#必须用cursor才行

    # sql = "select * from table_month_data where stock_code='"+stock_code+"' and trade_date>'"+ipo_date+"' ORDER BY trade_date ASC"
    sql = "select * from all_stocks_cleaned_factors_table"
    df = pd.read_sql(sql,conn)
    # print df["industry_type"]

    content = pd.DataFrame(df)

    return content



def clean_data():

    original_df = pandas_read_data_from_table()

    temp_df = copy.deepcopy(original_df)

    del temp_df["stock_code"]    
    del temp_df["industry_type"]
 
    # my_data = pd.read_csv('my_data.csv', sep=',')
    clean_data_df = autoclean(temp_df)

    del clean_data_df["index"]

    clean_data_df.insert(1,"stock_code",original_df["stock_code"])
    # 此处插入中文乱码
    # print original_df["industry_type"]
    # clean_data_df.insert(3,"industry_type",original_df["industry_type"])

    # clean_data_df.to_csv('clean_data_df.csv', sep=',', index=False)
    return clean_data_df

 

if __name__ == "__main__":
    # clean_data()
    # pandas_read_data_from_table()

    store_modified_data_into_new_table(clean_data())
    from add_industry_dummy_variables import get_add_insert_industry
    get_add_insert_industry(table_name = "final_all_stocks_cleaned_factors_table")
