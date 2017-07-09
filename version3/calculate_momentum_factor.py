#coding=utf-8
import sys
import MySQLdb
import datetime
import pandas as pd
from deal_with_day_data import *

reload(sys)
sys.setdefaultencoding('utf8')



def insert_data_into_table(table_name,insert_column,stock,date_list,data_list):
    db = MySQLdb.connect(host='127.0.0.1', user='root', passwd='zjz4818774', db='investv2', port=3306,charset='utf8')

    cursor = db.cursor()
    
    try:
        if len(date_list)==len(data_list):
            for i in range(len(date_list)):
                # print table_name,insert_column,data_list[i],date_list[i],stock
                if data_list[i] != None and data_list[i] != '' and pd.isnull(data_list[i]) == False:
                    sql = "UPDATE "+table_name+" SET "+insert_column+"="+str(data_list[i])+" WHERE trade_date='"+date_list[i]+" 00:00:00'"+" AND stock_code='"+stock+"'"
                    print sql
                    cursor.execute(sql)
        else:
            print "ERROR: insert data into table...len(date_list)!=len(data_list)"

        db.commit()
        print "update data successfully"
    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])

    cursor.close()
    db.close()

############################################计算动量因子#########################################################

def calculate_one_month_return(stock_code):
    one_month_return_list = [0]

    # stock_code = "000002.SZ"
    table_name = "table_day_data"
    all_close_price_list,all_date_list = get_date_close_list(stock_code,table_name)
    all_BOM,all_EOM = get_all_BOM_EOM()
    index_list = []
    for i in range(len(all_EOM)):

        index = all_date_list.index(all_EOM[i])
        index_list.append(index)
    # print index_list
    for i in range(len(index_list)-1):
        # print i
        # print index_list[i]
        if all_close_price_list[index_list[i]] != None and all_close_price_list[index_list[i+1]] != None:
            one_month_return = float((all_close_price_list[index_list[i+1]] - all_close_price_list[index_list[i]]))/all_close_price_list[index_list[i]]
        else:
            one_month_return = None
        one_month_return_list.append(one_month_return)
    
    insert_data_into_table("table_month_data","one_month_return",stock_code,all_EOM,one_month_return_list)

    return one_month_return_list

def calculate_three_month_return(stock_code):
    three_month_return_list = [0,0,0]
    # stock_code = "000002.SZ"
    table_name = "table_day_data"
    all_close_price_list,all_date_list = get_date_close_list(stock_code,table_name)
    all_BOM,all_EOM = get_all_BOM_EOM()
    index_list = []
    for i in range(len(all_EOM)):

        index = all_date_list.index(all_EOM[i])
        index_list.append(index)
    # print index_list
    for i in range(len(index_list)-3):
        # print i
        # print index_list[i]
        three_month_return = float((all_close_price_list[index_list[i+3]] - all_close_price_list[index_list[i]]))/all_close_price_list[index_list[i]]
        three_month_return_list.append(three_month_return)

    insert_data_into_table("table_month_data","three_month_return",stock_code,all_EOM,three_month_return_list)

    return three_month_return_list


# def calculate_six_month_return(stock_code,time_length):

################统一封装好的函数###############################

def calculate_month_return(stock_code,time_length):

    month_return_list = [0 for i in range(time_length) ]
    # stock_code = "000002.SZ"
    table_name = "table_day_data"
    all_close_price_list,all_date_list = get_date_close_list(stock_code,table_name)
    all_BOM,all_EOM = get_all_BOM_EOM()
    index_list = []
    for i in range(len(all_EOM)):

        index = all_date_list.index(all_EOM[i])
        index_list.append(index)
    # print index_list
    for i in range(len(index_list)-time_length):
        # print i
        # print index_list[i]
        month_return = ''

        if all_close_price_list[index_list[i+time_length]] != None and all_close_price_list[index_list[i]] != None:
            month_return = float((all_close_price_list[index_list[i+time_length]] - all_close_price_list[index_list[i]]))/all_close_price_list[index_list[i]]
        else:
            month_return = None

        month_return_list.append(month_return)

    insert_data_into_table("table_month_data",str(time_length)+"_month_return",stock_code,all_EOM,month_return_list)

    return month_return_list

def calculate_one_year_return():
    # one
    stock_code = "000002.SZ"
    table_name = "table_day_data"
    all_close_price_list,all_date_list = get_date_close_list(stock_code,table_name)
    all_BOM,all_EOM = get_all_BOM_EOM()
    index_list = []
    for i in range(len(all_EOM)):

        index = all_date_list.index(all_EOM[i])
        index_list.append(index)
    print index_list
    for i in range(len(index_list)-12):
        print i
        print index_list[i]
        print float((all_close_price_list[index_list[i+12]] - all_close_price_list[index_list[i]]))/all_close_price_list[index_list[i]]

def calculate_two_years_return():
    # one
    stock_code = "000002.SZ"
    table_name = "table_day_data"
    all_close_price_list,all_date_list = get_date_close_list(stock_code,table_name)
    all_BOM,all_EOM = get_all_BOM_EOM()
    index_list = []
    for i in range(len(all_EOM)):

        index = all_date_list.index(all_EOM[i])
        index_list.append(index)
    print index_list
    for i in range(len(index_list)-24):
        print i
        print index_list[i]
        print float((all_close_price_list[index_list[i+24]] - all_close_price_list[index_list[i]]))/all_close_price_list[index_list[i]]

def calculate_highest_one_day_return_of_month(stock_code):
    highest_one_day_return_list = []
    # stock_code = "000002.SZ"
    table_name = "table_day_data"
    all_close_price_list,all_date_list = get_date_close_list(stock_code,table_name)

    #计算2010到2016每一天的日收益率
    one_day_return_list = [0]

    for i in range(len(all_close_price_list)-1):

        one_day_return = ''
        
        if all_close_price_list[i+1] != None and all_close_price_list[i] != None:

            one_day_return = float(all_close_price_list[i+1]-all_close_price_list[i])/all_close_price_list[i]
        else:

            one_day_return = None

        one_day_return_list.append(one_day_return)

    print "one_day_return_list的长度是: ",len(one_day_return_list)

    #获取2010到2016的月初日期的列表和月末日期的列表
    all_BOM,all_EOM = get_all_BOM_EOM()
    
    BOM_index_list = []
    EOM_index_list = []

    for i  in range(len(all_BOM)):
        BOM_index_list.append(all_date_list.index(all_BOM[i]))
    print BOM_index_list

    # if len(all_BOM)==len(BOM_index_list):
        # print "len(all_BOM)==len(BOM_index_list)"

    for i  in range(len(all_EOM)):
        EOM_index_list.append(all_date_list.index(all_EOM[i]))
    print EOM_index_list

 
    if len(BOM_index_list)==len(EOM_index_list) and len(one_day_return_list)==len(all_date_list):
        print "len(BOM_index_list)==len(EOM_index_list)"

        for i in range(len(BOM_index_list)):

            highest_one_day_return = max(one_day_return_list[BOM_index_list[i]:(EOM_index_list[i]+1)])
            # print all_date_list[BOM_index_list[i]:(EOM_index_list[i]+1)]
            highest_one_day_return_list.append(highest_one_day_return)

        insert_data_into_table("table_month_data","highest_one_day_return_of_month",stock_code,all_EOM,highest_one_day_return_list)

    else:
        print "len(BOM_index_list)不等于len(EOM_index_list)，错误"
        return

    return highest_one_day_return_list

###############################################控制计算和存储#########################################################

def calculate_and_store_one_month_return(symbols): 
    db = MySQLdb.connect(host='127.0.0.1', user='root', passwd='zjz4818774', db='investv2', port=3306,charset='utf8')

    cursor = db.cursor()

    sql = "alter table table_month_data add one_month_return double"

    try:
        cursor.execute(sql)
        print "add key:one_month_return successfully"
    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        print "but this error will not cause wrong data, everything is ok"
    cursor.close()
    db.close()

    # symbols=['000002.SZ','000008.SZ','000009.SZ','000060.SZ','000063.SZ','000069.SZ','000100.SZ','000156.SZ','000157.SZ']#通过直接赋值获取股票代码用于测试
    for symbol in symbols:

        calculate_one_month_return(symbol)


def calculate_and_store_three_month_return(symbols): 
    db = MySQLdb.connect(host='127.0.0.1', user='root', passwd='zjz4818774', db='investv2', port=3306,charset='utf8')

    cursor = db.cursor()

    sql = "alter table table_month_data add three_month_return double"

    try:
        cursor.execute(sql)
        print "add key:three_month_return successfully"
    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        print "but this error will not cause wrong data, everything is ok"
    cursor.close()
    db.close()

    # symbols=['000002.SZ','000008.SZ','000009.SZ','000060.SZ','000063.SZ','000069.SZ','000100.SZ','000156.SZ','000157.SZ']#通过直接赋值获取股票代码用于测试
    for symbol in symbols:

        calculate_three_month_return(symbol)

###############################统一封装好的函数###############################
def calculate_and_store_month_return(symbols,time_length): 
    db = MySQLdb.connect(host='127.0.0.1', user='root', passwd='zjz4818774', db='investv2', port=3306,charset='utf8')

    cursor = db.cursor()

    sql = "alter table table_month_data add "+str(time_length)+"_month_return double"

    try:
        cursor.execute(sql)
        print "add key:"+str(time_length)+"_month_return successfully"
    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        print "but this error will not cause wrong data, everything is ok"
    cursor.close()
    db.close()

    # symbols=['000002.SZ','000008.SZ','000009.SZ','000060.SZ','000063.SZ','000069.SZ','000100.SZ','000156.SZ','000157.SZ']#通过直接赋值获取股票代码用于测试
    for symbol in symbols:

        calculate_month_return(symbol,time_length)
    
###################################控制计算和存储highest_one_day_return_of_month的函数##########################################
def calculate_and_store_highest_one_day_return_of_month(symbols): 
    db = MySQLdb.connect(host='127.0.0.1', user='root', passwd='zjz4818774', db='investv2', port=3306,charset='utf8')

    cursor = db.cursor()

    sql = "alter table table_month_data add highest_one_day_return_of_month double"

    try:
        cursor.execute(sql)
        print "add key:highest_one_day_return_of_month successfully"
    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        print "but this error will not cause wrong data, everything is ok"
    cursor.close()
    db.close()

    # symbols=['000002.SZ','000008.SZ','000009.SZ','000060.SZ','000063.SZ','000069.SZ','000100.SZ','000156.SZ','000157.SZ']#通过直接赋值获取股票代码用于测试
    for symbol in symbols:

        calculate_highest_one_day_return_of_month(symbol)


if __name__ == "__main__":
    
    # symbols=['000002.SZ','000008.SZ','000009.SZ','000060.SZ','000063.SZ','000069.SZ','000100.SZ','000156.SZ','000157.SZ']#通过直接赋值获取股票代码用于测试

    # calculate_and_store_one_month_return(symbols)
    # calculate_and_store_three_month_return(symbols)
    from stocks_pool_for_HZ300 import select_good_stocks

    symbols = select_good_stocks()

    calculate_and_store_month_return(symbols,1)
    calculate_and_store_month_return(symbols,3)
    calculate_and_store_month_return(symbols,6)
    calculate_and_store_month_return(symbols,12)
    calculate_and_store_month_return(symbols,24)
    calculate_and_store_highest_one_day_return_of_month(symbols)