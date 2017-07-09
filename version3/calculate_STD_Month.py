#coding=utf-8
import sys
import MySQLdb
import datetime
import pandas as pd
from deal_with_day_data import *

reload(sys)
sys.setdefaultencoding('utf8')

def calculate_daily_return_date_list(stock_code):

    all_close_price_list,all_date_list = get_date_close_list(stock_code,table_name="table_day_data")
    
    temp_1 = all_close_price_list[0:-1]
    temp_2 = all_close_price_list[1:]
    # temp_1 = [5,5,5,5,5]
    # temp_2 = [7.5,7.5,7.5,7.5,7.5]
    # temp_daily_return_list = map(lambda (a,b):(b-a)/a, zip(temp_1,temp_2))
    # daily_return_list = [0] + temp_daily_return_list


    temp_daily_return_list = []

    for i in range(len(temp_1)):

        if temp_1[i] != None and temp_2[i] != None:

            temp_daily_return = (temp_2[i] - temp_1[i]) / temp_1[i]
            
        else:
            temp_daily_return = None

        temp_daily_return_list.append(temp_daily_return)

    # temp_1 = pd.Series(temp_1)
    # temp_2 = pd.Series(temp_2)
    # # 针对两个list中数据都完整时，是可以的。但有数据为None的话，就会计算错误
    # # 所以用pandas处理

    daily_return_list = [0] + temp_daily_return_list


    return daily_return_list,all_date_list

def STD_function(input_list):
    temp = pd.Series(input_list)
    std = temp.std()
    return std


def insert_data_into_table(table_name,insert_column,stock,date_list,data_list):
    db = MySQLdb.connect(host='127.0.0.1', user='root', passwd='zjz4818774', db='invest_after_calculation', port=3306,charset='utf8')

    cursor = db.cursor()
    
    try:
        if len(date_list)==len(data_list):
            for i in range(len(date_list)):
                # print table_name,insert_column,data_list[i],date_list[i],stock
                if data_list[i] != None and pd.isnull(data_list[i]) == False:
                    sql = "UPDATE "+table_name+" SET "+insert_column+"="+str(data_list[i])+" WHERE trade_date='"+date_list[i]+" 00:00:00'"+" AND stock_code='"+stock+"'"
                    print sql
                    cursor.execute(sql)
                else:
                    print "this data is None , we don not need to update it"
        else:
            print "ERROR: insert data into table...len(date_list)!=len(data_list)"

        db.commit()
        print "update data successfully"
    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        print "but this error will not cause wrong data, everything is ok"

    cursor.close()
    db.close()

##################################### ######计算3、6个月#########################################################
############################################统一封装好的函数###############################

def calculate_std_month(stock_code,time_length):

    std_month_list = [0 for i in range(time_length-1) ]
    # stock_code = "000002.SZ"
    daily_return_list,all_date_list = calculate_daily_return_date_list(stock_code)
    #获取2010到2016的月初日期的列表和月末日期的列表
    all_BOM,all_EOM = get_all_BOM_EOM()
    
    BOM_index_list = []
    EOM_index_list = []

    for i  in range(len(all_BOM)):
        BOM_index_list.append(all_date_list.index(all_BOM[i]))

    for i  in range(len(all_EOM)):
        EOM_index_list.append(all_date_list.index(all_EOM[i]))
    # print EOM_index_list

    if len(BOM_index_list)==len(EOM_index_list):
        print "len(BOM_index_list)==len(EOM_index_list)"

        for i in range(len(BOM_index_list)-(time_length-1)):
            # EOM_index 里面在all_close_price_list和all_date_list中的位置,EOM_index_list[i]中的i是EOM_index_list中的位置
            # EOM_index_list[i+2]+1 加1是因为切片是"取头不取尾"
            std_month = STD_function(daily_return_list[BOM_index_list[i]:(EOM_index_list[i+(time_length-1)]+1)])
            
            std_month_list.append(std_month)

    else:
        print "calculate_std_month执行出错error"
        return

    insert_data_into_table("table_month_data",str(time_length)+"_month_std",stock_code,all_EOM,std_month_list)

    return std_month_list

###############################################控制计算和存储#########################################################
##############################################统一封装好的函数########################################################
def calculate_and_store_std_month(symbols,time_length): 
    db = MySQLdb.connect(host='127.0.0.1', user='root', passwd='zjz4818774', db='invest_after_calculation', port=3306,charset='utf8')

    cursor = db.cursor()

    sql = "alter table table_month_data add "+str(time_length)+"_month_std double"

    try:
        cursor.execute(sql)
        print "add key:"+str(time_length)+"_month_std successfully"
    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        print "but this error will not cause wrong data, everything is ok"
    cursor.close()
    db.close()

    # symbols=['000002.SZ','000008.SZ','000009.SZ','000060.SZ','000063.SZ','000069.SZ','000100.SZ','000156.SZ','000157.SZ']#通过直接赋值获取股票代码用于测试
    for symbol in symbols:

        calculate_std_month(symbol,time_length)

if __name__ == "__main__":
    
    # symbols=['000002.SZ','000008.SZ','000009.SZ','000060.SZ','000063.SZ','000069.SZ','000100.SZ','000156.SZ','000157.SZ']#通过直接赋值获取股票代码用于测试
    from stocks_pool_for_HZ300 import select_good_stocks

    symbols = select_good_stocks()
    # a_list = [5 for i in range(100)]+ [0 for i in range(100)]
    # print STD_function(a_list)
    # print pd.Series(a_list).std()
    
    calculate_and_store_std_month(symbols,1)
    calculate_and_store_std_month(symbols,3)
    calculate_and_store_std_month(symbols,6)

