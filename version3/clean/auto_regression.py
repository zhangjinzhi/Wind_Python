#coding:utf-8
import pandas as pd
import pymysql
import MySQLdb
import datetime,time
import numpy as np
import pandas as pd
import copy
import numpy as np
import statsmodels.api as sm
import csv

def get_factor_MonthReturn_from_table(factor,MonthReturn,table_name,trade_date):
    db = MySQLdb.connect(host='127.0.0.1', user='root', passwd='zjz4818774', db='invest_lastest', port=3306,charset='utf8')
    cursor = db.cursor()
    sql = "select "+factor+","+MonthReturn+" from "+table_name+" where trade_date='"+trade_date+"'"
    # sql = SELECT trade_date, stock_code, 1_month_return, mkt_cap_ard FROM all_stocks_cleaned_factors_table WHERE trade_date = '2016-01-031 00:00:00'
    
    x_list = []
    y_list = []
    try:
        cursor.execute(sql)
        rows= cursor.fetchall()
        
        for row in rows:
        	x_list.append(row[0])
        	y_list.append(row[1])

    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])

    cursor.close()
    db.close()

    return x_list,y_list


def output_tvalues_correlation(x_list,y_list):

    model = sm.OLS(y_list,x_list)
    
    results = model.fit()
    
    # print results.params

    # print results.summary()

    # print 't: ', results.tvalues[1]
    # 
    from scipy.stats.stats import pearsonr
    correlation = pearsonr(x_list[:-1], y_list[1:])[0]

    return results.tvalues[0],correlation

def store_tvalues_correlation(outputFp,x_list,y_list,trade_date,factor):
    
    tvalues,correlation = output_tvalues_correlation(x_list,y_list)
    # result_df['factor']     = factor
    # result_df['trade_date'] = trade_date
    # result_df['tvalues']    = tvalues
    # print result_df['factor']
    # print result_df['trade_date']
    # print result_df['tvalues']
    result_list = []
    result_list.append(factor)
    result_list.append(trade_date)
    result_list.append(tvalues)
    result_list.append(correlation)
    # columns_text = ['factor','trade_date','tvalues']   
    # print result_list
    # result_df = pd.DataFrame(result_list,columns=columns_text)
    # print result_df
    # result_df.to_csv('test.csv', mode='ab+',index=False,dtype={'factor':str,'trade_date':str})

    # outputFp = open('test.csv', 'ab+')
    csvWriter = csv.writer(outputFp, dialect='excel')
    csvWriter.writerow(result_list)
    # outputFp.close()



def get_2010_to_2016_all_EOM():
    import sys
    sys.path.append('../')
    from deal_with_day_data import get_first_end_day_of_month
    we_select_years = range(2010,2017)
    # print we_select_years
    all_first_day_of_month_many_years = []
    all_end_day_of_month_many_years = []

    for year in we_select_years:
        first_day_of_month,end_day_of_month= get_first_end_day_of_month(year)
        all_first_day_of_month_many_years += first_day_of_month
        all_end_day_of_month_many_years += end_day_of_month

    return all_end_day_of_month_many_years


def auto_regression_get_tvalues_correlation():

    columns_text = ['factor','trade_date','tvalues','correlation']   
    outputFp = open('test.csv', 'ab+')
    csvWriter = csv.writer(outputFp, dialect='excel')
    csvWriter.writerow(columns_text)
    # outputFp.close()

    all_EOM_2010_to_2016 = get_2010_to_2016_all_EOM()

    ####################手动添加需要计算的因子#############################
    #这些因子的t值和correlation值会被一一计算,所以需要计算的因子都可以直接放到这里,程序运行即可
    factor_list = ["mkt_cap_ard","or_ttm"]
    
    for factor in factor_list:

        for EOM in all_EOM_2010_to_2016:
    
            MonthReturn,table_name = "1_month_return","all_stocks_cleaned_factors_table"
    
            trade_date = EOM
    
            x_list,y_list = get_factor_MonthReturn_from_table(factor,MonthReturn,table_name,trade_date)
    
            store_tvalues_correlation(outputFp,x_list,y_list,trade_date,factor)
    

    outputFp.close()

if __name__ == "__main__":
    
    # factor,MonthReturn,table_name,trade_date = "mkt_cap_ard","1_month_return","all_stocks_cleaned_factors_table","2016-01-031 00:00:00"

    # x_list,y_list = get_factor_MonthReturn_from_table(factor,MonthReturn,table_name,trade_date)
    # x_list,y_list = [1,2,3,4,5] , [10,9,8,7,6]
    # tvalues,correlation = output_tvalues_correlation(x_list,y_list)
    # print tvalues
    # print correlation
    # 
    # # get_2010_to_2016_all_EOM()
    # 
    auto_regression_get_tvalues_correlation()