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

def get_factorone_factortwo_from_table(factorone,factortwo,table_name,trade_date):
    db = MySQLdb.connect(host='127.0.0.1', user='root', passwd='zjz4818774', db='invest_latest', port=3306,charset='utf8')
    cursor = db.cursor()
    #在这里指定取出的数据的顺序，是为了和get_dummy（）中的dummy_df.sort_values(['stock_code'])保持一致，
    #要么同时根据stock_code按序排列;要么都不根据stock_code按序排列，那么就会都按数据库的默认存储顺序排列
    sql = "select "+factorone+","+factortwo+" from "+table_name+" where trade_date='"+trade_date+"' ORDER BY stock_code ASC"
    # sql = SELECT trade_date, stock_code, 1_month_return, mkt_cap_ard FROM all_stocks_cleaned_factors_table WHERE trade_date = '2016-01-031 00:00:00'

    x_list = []
    y_list = []
    try:
        cursor.execute(sql)
        rows= cursor.fetchall()
        
        for row in rows:
            if row[0] != None and pd.isnull(row[0]) == False and row[1] != None and pd.isnull(row[1]) == False:
        	    x_list.append(row[0])
        	    y_list.append(row[1])

    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])

    cursor.close()
    db.close()

    return x_list,y_list

def get_one_month_return_list(trade_date):
    db = MySQLdb.connect(host='127.0.0.1', user='root', passwd='zjz4818774', db='invest_latest', port=3306,charset='utf8')
    cursor = db.cursor()
    #在这里指定取出的数据的顺序，是为了和get_dummy（）中的dummy_df.sort_values(['stock_code'])保持一致，
    #要么同时根据stock_code按序排列;要么都不根据stock_code按序排列，那么就会都按数据库的默认存储顺序排列
    table_name = "final_all_stocks_cleaned_factors_table" 
    sql = "select 1_month_return from "+table_name+" where trade_date='"+trade_date+"' ORDER BY stock_code ASC"
    # sql = SELECT trade_date, stock_code, 1_month_return, mkt_cap_ard FROM all_stocks_cleaned_factors_table WHERE trade_date = '2016-01-031 00:00:00'

    x_list = []
    try:
        cursor.execute(sql)
        rows= cursor.fetchall()
        
        for row in rows:
            if row[0] != None and pd.isnull(row[0]) == False:
        	    x_list.append(row[0])

    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])

    cursor.close()
    db.close()

    return x_list

def get_dummy(trade_date):
    from add_industry_dummy_variables import add_dummy_avariable

    dummy_df = add_dummy_avariable(table_name="final_all_stocks_cleaned_factors_table")
    # print dummy_df.values[:,1:]
    dummy_df = dummy_df[dummy_df.trade_date==trade_date]
    #在这里指定数据的顺序，是为了和get_factor_MonthReturn_from_table中的sql语句的ORDER BY stock_code ASC保持一致，
    #要么同时根据stock_code按序排列;要么都不根据stock_code按序排列，那么就会都按数据库的默认存储顺序排列
    dummy_df = dummy_df.sort_values(['stock_code'])
    #print dummy_df
    del dummy_df['trade_date']
    del dummy_df['stock_code']
    # print dummy_df
    dummy = dummy_df.values
    #print "lenn(dummy) is: ",len(dummy)
    return dummy


def get_residual(x_list,y_list,trade_date):

    dummy = get_dummy(trade_date)
    print "len(dummy) and dummy  is : ",len(dummy)
    #print dummy
    print "len(dummy[:,1:]) and  dummy[:,1:] is : ",len(dummy[:,1:])
    #print dummy[:,1:]
    print "len(x_list) and xlist is : ",len(x_list)
    #print x_list

    # drop reference category
    X_list = np.column_stack((x_list, dummy[:,1:]))

    X_list = sm.add_constant(X_list, prepend=False)
    model = sm.OLS(y_list,X_list)
    results = model.fit()

    residual_list = results.resid
    print "len(residual) is : ",len(residual_list)

    return residual_list

def output_correlation(x_list,y_list,trade_date):
    
    residual_list = get_residual(x_list,y_list,trade_date)
    
    one_month_return_list = get_one_month_return_list(trade_date)
    from scipy.stats.stats import pearsonr
    # 相关性系数，不加常数项
    
    correlation = pearsonr(residual_list[:-1], one_month_return_list[1:])[0]
    
    #print correlation
    return correlation

def output_WLS_tvalues_correlation(x_list,y_list):

    X_list = sm.add_constant(x_list)
    model = sm.WLS(y_list,X_list) #model = sm.WLS(y_list,X_list,weights=1./w)
    results = model.fit()

    print results.summary()
    
    print results.params

    # print 't: ', results.tvalues[1]
    # 相关性系数，加了常数项
    correlation = results.params[1]

    from scipy.stats.stats import pearsonr
    # 相关性系数，不加常数项
    correlation = pearsonr(x_list[:-1], y_list[1:])[0]
    
    return results.tvalues[1],correlation


def output_RLM_tvalues_correlation(x_list,y_list):

    X_list = sm.add_constant(x_list)
    rlm_model = sm.RLM(y_list, X_list, M=sm.robust.norms.HuberT())
    rlm_results = rlm_model.fit()

    print rlm_results.summary()
    
    print rlm_results.params

    # 相关性系数，加了常数项
    correlation = rlm_results.params[1]

    from scipy.stats.stats import pearsonr
    # 相关性系数，不加常数项
    correlation = pearsonr(x_list[:-1], y_list[1:])[0]

    return rlm_results.tvalues[1],correlation

def store_tvalues_correlation(outputFp,x_list,y_list,trade_date,factor):
    
    correlation = output_correlation(x_list,y_list,trade_date)
    # result_df['factor']     = factor
    # result_df['trade_date'] = trade_date
    # result_df['tvalues']    = tvalues
    # print result_df['factor']
    # print result_df['trade_date']
    # print result_df['tvalues']
    result_list = []
    result_list.append(factor)
    result_list.append(trade_date)
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

    columns_text = ['factor','trade_date','correlation']   
    outputFp = open('output_correlation.csv', 'ab+')
    csvWriter = csv.writer(outputFp, dialect='excel')
    csvWriter.writerow(columns_text)
    # outputFp.close()

    all_EOM_2010_to_2016 = get_2010_to_2016_all_EOM()

    ####################手动添加需要计算的因子#############################
    #这些因子的t值和correlation值会被一一计算,所以需要计算的因子都可以直接放到这里,程序运行即可
    factor_list = ["close","profit_ttm_to_mkt_cap_ard","bps_to_mkt_close","or_ttm_to_mkt_cap_ard","fcff_to_mkt_cap_ard","cfps_ttm_to_mkt_cap_ard","wgsd_com_eq_to_mkt_cap_ard","peg","ev2_to_ebitda","mkt_cap_ard","log_mkt_cap_ard","mkt_cap_float","log_mkt_cap_float","mkt_cap_float_to_mkt_cap_ard","wgsd_assets","log_wgsd_assets","yoy_or","yoyprofit","qfa_yoyprofit","grossprofitmargin_growthrate","eps_ttm_growthrate","roe_ttm2_growthrate","roa_ttm2_growthrate","yoybps","yoy_assets","wgsd_yoyocf","roe","roe_ttm2","roa","roa_ttm2","grossprofitmargin","grossprofitmargin_ttm2","assetsturn","faturn","op_ttm2","current","cashtocurrentdebt","quick","assets_to_com_eq_paholder","longdebt_to_com_eq_paholder","debttoassets","1_month_return","3_month_return","6_month_return","12_month_return","24_month_return","highest_one_day_return_of_month","highest_to_lowest_of_one_month","highest_to_lowest_of_three_month","highest_to_lowest_of_six_month","1_month_std","3_month_std","6_month_std","amt_to_1_month_std","amt_to_3_month_std","amt_to_6_month_std","MACD","VMACD","SOBV","RSI","turn","3_month_turn","6_month_turn","dividendyield_to_mkt_cap_ard","wgsd_oper_cf_to_mkt_cap_ard","rating_upgrade","rating_downgrade","rating_instnum","rating_change","wrating_targetprice","rating_diff_change"]
    
    for factor in factor_list:

        for EOM in all_EOM_2010_to_2016:
    
            factorone,table_name = "mkt_cap_ard","final_all_stocks_cleaned_factors_table"
    
            trade_date = EOM

            x_list,y_list = get_factorone_factortwo_from_table(factorone,factor,table_name,trade_date)
    
            #store_tvalues_correlation(outputFp,x_list,y_list,trade_date,factor)
            try:
                store_tvalues_correlation(outputFp,x_list,y_list,trade_date,factor)
                #raise MyException("store_tvalue_correlation wrong, error in dummy merge xlist, length is not the same")
            except:
                print EOM
                print factor
                print "store_tvalue_correlation wrong, error in dummy merge xlist, length is not the same"
    

    outputFp.close()

if __name__ == "__main__":
    
    # factor,MonthReturn,table_name,trade_date = "mkt_cap_ard","1_month_return","all_stocks_cleaned_factors_table","2016-01-031 00:00:00"

    # x_list,y_list = get_factor_MonthReturn_from_table(factor,MonthReturn,table_name,trade_date)
    ######################################################################################
    #x,y = [1,2,100,4,5] , [10,11,8,9,6]
    #X = sm.add_constant(x)    
    #model = sm.OLS(y, X)
    #results = model.fit()
    #print results.resid
    
    # tvalues,correlation = output_WLS_tvalues_correlation(x_list,y_list)
    # print tvalues
    # print correlation
    ####################################################################################
    # # get_2010_to_2016_all_EOM()
    

    auto_regression_get_tvalues_correlation()
    

    ####################################################################################
    # nsample = 50
    # groups = np.zeros(nsample, int)
    # groups[20:40] = 1
    # groups[40:] = 2
    # print groups
    # #################dummy = (groups[:,None] == np.unique(groups)).astype(float)
    
    # dummy = sm.categorical(groups, drop=True)
    # print len(dummy)
    # print len(dummy[:,1:])
    # x = np.linspace(0, 20, nsample)
    # print "x is : "
    # print len(x)
    # # ######################### drop reference category
    # X = np.column_stack((x, dummy[:,1:]))
    # X = sm.add_constant(X, prepend=False)
    
    # beta = [1., 3, -3, 10]
    # y_true = np.dot(X, beta)
    # e = np.random.normal(size=nsample)
    # y = y_true + e

    # # print(X[:5,:])
    # # print(y[:5])
    # # print(groups)
    # # print(dummy[:5,:])
    ##################################################################################### 

    ######################################################################################
