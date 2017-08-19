#coding=utf-8
import sys
import MySQLdb
import datetime

from deal_with_day_data import *

reload(sys)
sys.setdefaultencoding('utf8')


def insert_data_into_table(table_name,insert_column,stock,date_list,data_list):
    db = MySQLdb.connect(host='127.0.0.1', user='root', passwd='zjz4818774', db='invest_latest', port=3306,charset='utf8')

    cursor = db.cursor()
    
    try:
        if len(date_list)==len(data_list):
            for i in range(len(date_list)):
                # print table_name,insert_column,data_list[i],date_list[i],stock
                if data_list[i] != None:

                    sql = "UPDATE "+table_name+" SET "+insert_column+"="+str(data_list[i])+" WHERE trade_date='"+date_list[i]+" 00:00:00'"+" AND stock_code='"+stock+"'"
                    # print sql
                    cursor.execute(sql)
                # else:
                #     print "date is None , do not store in table"
        else:
            print "ERROR: insert data into table...len(date_list)!=len(data_list)"

        db.commit()
        print "update data successfully"
    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        print "but this error will not cause wrong data, everything is ok"

    cursor.close()
    db.close()
############################################计算动波动因子#########################################################

def calculate_highest_to_lowest_of_one_month(stock_code):
    highest_to_lowest_list = []
    # stock_code = "000002.SZ"
    table_name = "table_day_data"
    all_close_price_list,all_date_list = get_date_close_list(stock_code,table_name)

    #获取2010到2016的月初日期的列表和月末日期的列表
    all_BOM,all_EOM = get_all_BOM_EOM()
    
    BOM_index_list = []
    EOM_index_list = []

    for i  in range(len(all_BOM)):
        BOM_index_list.append(all_date_list.index(all_BOM[i]))
    # print BOM_index_list

    for i  in range(len(all_EOM)):
        EOM_index_list.append(all_date_list.index(all_EOM[i]))

    print "len(EOM_index_list) is : ",len(EOM_index_list)
 
    if len(BOM_index_list)==len(EOM_index_list):
        # print "len(BOM_index_list)==len(EOM_index_list)"

        for i in range(len(BOM_index_list)):

            max_daily_stock_price = max(all_close_price_list[BOM_index_list[i]:(EOM_index_list[i]+1)])
            # print all_date_list[BOM_index_list[i]:(EOM_index_list[i]+1)]
            min_daily_stock_price = min(all_close_price_list[BOM_index_list[i]:(EOM_index_list[i]+1)])
            # print max_daily_stock_price
            # print min_daily_stock_price
            highest_to_lowest = ''
            if max_daily_stock_price != None and min_daily_stock_price != None:

                highest_to_lowest = max_daily_stock_price/min_daily_stock_price
            else:

                highest_to_lowest = None

            highest_to_lowest_list.append(highest_to_lowest) 

    else:
        print "calculate_highest_to_lowest_of_one_month执行出错error"
        return
    
    insert_data_into_table("table_month_data","highest_to_lowest_of_one_month",stock_code,all_EOM,highest_to_lowest_list)

    return highest_to_lowest_list

def calculate_highest_to_lowest_of_three_month(stock_code):
    highest_to_lowest_of_three_month_list = [0,0]
    # stock_code = "000002.SZ"
    table_name = "table_day_data"
    all_close_price_list,all_date_list = get_date_close_list(stock_code,table_name)

    #获取2010到2016的月初日期的列表和月末日期的列表
    all_BOM,all_EOM = get_all_BOM_EOM()
    
    BOM_index_list = []
    EOM_index_list = []

    for i  in range(len(all_BOM)):
        BOM_index_list.append(all_date_list.index(all_BOM[i]))
    # print BOM_index_list

    # if len(all_BOM)==len(BOM_index_list):
        # print "len(all_BOM)==len(BOM_index_list)"

    for i  in range(len(all_EOM)):
        EOM_index_list.append(all_date_list.index(all_EOM[i]))
    # print EOM_index_list

 
    if len(BOM_index_list)==len(EOM_index_list):
        print "len(BOM_index_list)==len(EOM_index_list)"

        for i in range(len(BOM_index_list)-2):
            # EOM_index 里面在all_close_price_list和all_date_list中的位置,EOM_index_list[i]中的i是EOM_index_list中的位置
            # EOM_index_list[i+2]+1 加1是因为切片是"取头不取尾"
            max_three_month_stock_price = max(all_close_price_list[BOM_index_list[i]:(EOM_index_list[i+2]+1)])
            # print all_date_list[BOM_index_list[i]:(EOM_index_list[i]+1)]
            min_three_month_stock_price = min(all_close_price_list[BOM_index_list[i]:(EOM_index_list[i+2]+1)])
            
            highest_to_lowest = ''
            if max_three_month_stock_price != None and min_three_month_stock_price != None:
                highest_to_lowest = max_three_month_stock_price/min_three_month_stock_price
            else:
                highest_to_lowest = None
            highest_to_lowest_of_three_month_list.append(highest_to_lowest)

    else:
        print "calculate_highest_to_lowest_of_three_month执行出错error"
        return

    insert_data_into_table("table_month_data","highest_to_lowest_of_three_month",stock_code,all_EOM,highest_to_lowest_of_three_month_list)

    return highest_to_lowest_of_three_month_list


def calculate_highest_to_lowest_of_six_month(stock_code):
    highest_to_lowest_of_six_month_list = [0,0,0,0,0]
    # stock_code = "000002.SZ"
    table_name = "table_day_data"
    all_close_price_list,all_date_list = get_date_close_list(stock_code,table_name)

    #获取2010到2016的月初日期的列表和月末日期的列表
    all_BOM,all_EOM = get_all_BOM_EOM()
    
    BOM_index_list = []
    EOM_index_list = []

    for i  in range(len(all_BOM)):
        BOM_index_list.append(all_date_list.index(all_BOM[i]))
    # print BOM_index_list

    # if len(all_BOM)==len(BOM_index_list):
        # print "len(all_BOM)==len(BOM_index_list)"

    for i  in range(len(all_EOM)):
        EOM_index_list.append(all_date_list.index(all_EOM[i]))
    # print EOM_index_list

 
    if len(BOM_index_list)==len(EOM_index_list):
        print "len(BOM_index_list)==len(EOM_index_list)"

        for i in range(len(BOM_index_list)-5):
            # EOM_index 里面在all_close_price_list和all_date_list中的位置,EOM_index_list[i]中的i是EOM_index_list中的位置
            # EOM_index_list[i+2]+1 加1是因为切片是"取头不取尾"
            max_six_month_stock_price = max(all_close_price_list[BOM_index_list[i]:(EOM_index_list[i+5]+1)])
            # print all_date_list[BOM_index_list[i]:(EOM_index_list[i]+1)]
            min_six_month_stock_price = min(all_close_price_list[BOM_index_list[i]:(EOM_index_list[i+5]+1)])

            highest_to_lowest = ''
            if max_six_month_stock_price != None and min_six_month_stock_price != None:

                highest_to_lowest = max_six_month_stock_price/min_six_month_stock_price
            else:

                highest_to_lowest = None

            highest_to_lowest_of_six_month_list.append(highest_to_lowest)
            
    else:
        print "calculate_highest_to_lowest_of_six_month执行出错error"
        return

    insert_data_into_table("table_month_data","highest_to_lowest_of_six_month",stock_code,all_EOM,highest_to_lowest_of_six_month_list)

    return highest_to_lowest_of_six_month_list


def calculate_and_store_highest_to_lowest_of_one_month(symbols): 
    db = MySQLdb.connect(host='127.0.0.1', user='root', passwd='zjz4818774', db='invest_latest', port=3306,charset='utf8')

    cursor = db.cursor()

    sql = "alter table table_month_data add highest_to_lowest_of_one_month double"

    try:
        cursor.execute(sql)
        print "add key:highest_to_lowest_of_one_month successfully"
    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        print "but this error will not cause wrong data, everything is ok"
    cursor.close()
    db.close()

    # symbols=['000002.SZ','000008.SZ','000009.SZ','000060.SZ','000063.SZ','000069.SZ','000100.SZ','000156.SZ','000157.SZ']#通过直接赋值获取股票代码用于测试
    for symbol in symbols:

        calculate_highest_to_lowest_of_one_month(symbol)
    

def calculate_and_store_highest_to_lowest_of_three_month(symbols):   
    db = MySQLdb.connect(host='127.0.0.1', user='root', passwd='zjz4818774', db='invest_latest', port=3306,charset='utf8')

    cursor = db.cursor()

    sql = "alter table table_month_data add highest_to_lowest_of_three_month double"

    try:
        cursor.execute(sql)
        print "add key:highest_to_lowest_of_three_month successfully"
    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        print "but this error will not cause wrong data, everything is ok"
    cursor.close()
    db.close()

    # symbols=['000002.SZ','000008.SZ','000009.SZ','000060.SZ','000063.SZ','000069.SZ','000100.SZ','000156.SZ','000157.SZ']#通过直接赋值获取股票代码用于测试
    for symbol in symbols:

        calculate_highest_to_lowest_of_three_month(symbol)

def calculate_and_store_highest_to_lowest_of_six_month(symbols):
    db = MySQLdb.connect(host='127.0.0.1', user='root', passwd='zjz4818774', db='invest_latest', port=3306,charset='utf8')

    cursor = db.cursor()

    sql = "alter table table_month_data add highest_to_lowest_of_six_month double"

    try:
        cursor.execute(sql)
        print "add key:highest_to_lowest_of_six_month successfully"
    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        print "but this error will not cause wrong data, everything is ok"
    cursor.close()
    db.close()

    # symbols=['000002.SZ','000008.SZ','000009.SZ','000060.SZ','000063.SZ','000069.SZ','000100.SZ','000156.SZ','000157.SZ']#通过直接赋值获取股票代码用于测试
    for symbol in symbols:

        calculate_highest_to_lowest_of_six_month(symbol)

if __name__ == "__main__":
    # symbols=['000002.SZ','000008.SZ','000009.SZ','000060.SZ','000063.SZ','000069.SZ','000100.SZ','000156.SZ','000157.SZ']#通过直接赋值获取股票代码用于测试
    
    from stocks_pool_for_HZ300 import select_good_stocks

    symbols = select_good_stocks()
    
    calculate_and_store_highest_to_lowest_of_one_month(symbols)
    calculate_and_store_highest_to_lowest_of_three_month(symbols)
    calculate_and_store_highest_to_lowest_of_six_month(symbols)
  
