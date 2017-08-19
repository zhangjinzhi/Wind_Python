#coding:utf-8
import pandas as pd
import pymysql
import MySQLdb
import datetime,time
import numpy as np
import pandas as pd
import copy
from datacleaner import autoclean

def get_ipo_date_from_table(table_name,stock_code):
    # table_name = hz300_stocks
    db = MySQLdb.connect(host='127.0.0.1', user='root', passwd='zjz4818774', db='invest_latest', port=3306,charset='utf8')
    cursor = db.cursor()
    sql = "select CAST(ipo_date AS CHAR) AS ipo_date from "+table_name+" where hz300_code='"+stock_code+"'"
    print sql
    try:
        cursor.execute(sql)
        ipo_date= cursor.fetchone()
    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
    cursor.close()
    db.close()
    return "".join(ipo_date)


def compare_date(ipo_date,given_date):

    ipo_date_time = time.mktime(time.strptime(ipo_date,'%Y-%m-%d %H:%M:%S'))

    given_date_time = time.mktime(time.strptime(given_date,'%Y-%m-%d %H:%M:%S'))
   
    # print "ipo_date_time   is :",ipo_date_time
    # print "given_date_time is :",given_date_time

    if float(ipo_date_time) > float(given_date_time):

        return ipo_date
    else:
        return given_date


def pandas_read_data_from_table(stock_code,date):

    conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='zjz4818774', db='invest_latest')
    cursor = conn.cursor()
    # cursor.execute("DROP TABLE IF EXISTS test")#必须用cursor才行

    # sql = "select * from table_month_data where stock_code='"+stock_code+"' and trade_date>'"+ipo_date+"' ORDER BY trade_date ASC"
    sql = "select * from table_month_data where stock_code='"+stock_code+"' and trade_date>'"+date+"' ORDER BY trade_date ASC"
    print sql
    df = pd.read_sql(sql,conn)
    
    content = pd.DataFrame(df)
    
    return content


def mad_based_outlier(points, k = 1.4826):
    # 有关 k = 1.4826，具体参考https://en.wikipedia.org/wiki/Median_absolute_deviation
    tmp = copy.deepcopy(points)
    median = np.median(points, axis=0)
    diff = np.abs(points - median)
    #print(diff)
    mad = np.median(diff)
    std = k * mad

    maxval, minval = median + 1.96*std, median - 1.96*std
    # print('mad-based outliers: ',np.sum(points < minval) +np.sum(points > maxval))
    # print('minval, maxval: ',minval, maxval)

    for  item in tmp:
        if item > maxval:
            item = maxval
        if item < minval:
            item =minval

    # tmp[tmp<minval] = minval
    # tmp[tmp>maxval] = maxval

    # return tmp, (points < minval) | (points > maxval)
    return tmp

# def Z_ScoreNormalization(x,mean,std):  
#     x = (x - mean) / std 
#     return x

def clean_data(original_df):
    #mad方法
    temp_df = copy.deepcopy(original_df)
    del temp_df["index"]
    del temp_df["trade_date"]
    del temp_df["stock_code"]    
    del temp_df["trade_status"]
    del temp_df["data_source"]
    del temp_df["created_date"]
    #用datacleaner的autoclean方法处理缺失值
    #只要某列有数据不为空，那么最后一定会补全整列数据。
    #如果整列为空，那么则整列无法修补数据
    # temp_df.loc[10:20, 'ev2_to_ebitda'] = 30
    clean_data_df = autoclean(temp_df)
    # clean_data_df.to_csv('auto_clean.csv', sep=',', index=False)
    # print clean_data_df
#############由于季度数据得到的增长率，出现有规律的0值,需要填充##############################
    for i in range(len(clean_data_df["grossprofitmargin_growthrate"])):
        if clean_data_df["grossprofitmargin_growthrate"][i] != 0 and pd.isnull(clean_data_df["grossprofitmargin_growthrate"][i]) == False:
            clean_data_df["grossprofitmargin_growthrate"][i-1] = clean_data_df["grossprofitmargin_growthrate"][i]
            clean_data_df["grossprofitmargin_growthrate"][i-2] = clean_data_df["grossprofitmargin_growthrate"][i]

    for i in range(len(clean_data_df["roe_ttm2_growthrate"])):
        if clean_data_df["roe_ttm2_growthrate"][i] != 0 and pd.isnull(clean_data_df["roe_ttm2_growthrate"][i]) == False:
            clean_data_df["roe_ttm2_growthrate"][i-1] = clean_data_df["roe_ttm2_growthrate"][i]
            clean_data_df["roe_ttm2_growthrate"][i-2] = clean_data_df["roe_ttm2_growthrate"][i]

    for i in range(len(clean_data_df["roa_ttm2_growthrate"])):
        if clean_data_df["roa_ttm2_growthrate"][i] != 0 and pd.isnull(clean_data_df["roa_ttm2_growthrate"][i]) == False:
            clean_data_df["roa_ttm2_growthrate"][i-1] = clean_data_df["roa_ttm2_growthrate"][i]
            clean_data_df["roa_ttm2_growthrate"][i-2] = clean_data_df["roa_ttm2_growthrate"][i]

    # clean_data_df.to_csv('auto_clean1.csv', sep=',', index=False)
###############################################################################
    # print clean_data_df.columns
    for column in clean_data_df.columns:
        clean_data_df[column] = mad_based_outlier(clean_data_df[column])
        # print max(clean_data_df[column])
    #标准化
    #方法一
    # clean_data_df = clean_data_df.apply(lambda x: (x - np.min(x)) / (np.max(x) - np.min(x))) 
    #方法二
    # (clean_data_df - clean_data_df.min()) / (clean_data_df.max() - clean_data_df.min())
    #Z标准化
    clean_data_df = clean_data_df.apply(lambda x: (x - np.average(x)) / np.std(x)) 

    # clean_data_df.to_csv('clean_data_df1.csv', sep=',', index=False)

    return clean_data_df

def store_modified_data_into_new_table(clean_data_df):
    #存储数据
    from sqlalchemy import create_engine

    table_name = "all_stocks_cleaned_factors_table"
    engine = create_engine('mysql://root:zjz4818774@localhost/invest_latest?charset=utf8')
    try:
        clean_data_df.to_sql(table_name,engine,if_exists='append');
    except Exception as e:
        #如果写入数据库失败，写入日志表，便于后续分析处理
        error_log=pd.DataFrame()
        error_log['error_type']="store_cleaned_data_into_new_table"
        error_log['table']=table_name
        error_log['error_info']=e
        error_log['created_date']=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        #error_log.to_sql('store_data_error_log',engine,if_exists='append')
        print   self.getCurrentTime(),": SQL Exception :%s" % (e)

def get_clean_store(stock_code,table_name,given_date):

    # stock_code = "601998.SH"
    ipo_date = get_ipo_date_from_table(table_name,stock_code)
    date = compare_date(ipo_date,given_date)
    original_df = pandas_read_data_from_table(stock_code,date)

    clean_data_df = clean_data(original_df)

    clean_data_df.insert(0,"trade_date",original_df["trade_date"])
    clean_data_df.insert(1,"stock_code",original_df["stock_code"])

    # print clean_data_df

    store_modified_data_into_new_table(clean_data_df)

if __name__ == "__main__":

######python 默认是浅拷贝 #######
    # a_list = [1,2,3,4,5,3,2]
    # b_list  = a_list #浅拷贝
    # b_list  = copy.deepcopy(a_list) #深拷贝
    # a_list.append("a")
    # print a_list
    # print b_list
#################################
    # a_list = [1,2,3,4,5,6,7]
    # print mad_based_outlier(a_list)
#################################
    import sys
    sys.path.append('../')

    from stocks_pool_for_HZ300 import select_good_stocks

    table_name = "hz300_stocks"
    
    given_date = "2010-01-01 00:00:00"

    symbols = select_good_stocks()

    for stock_code in symbols:

        get_clean_store(stock_code,table_name,given_date)

        # print pandas_read_data_from_table(stock_code,date)
