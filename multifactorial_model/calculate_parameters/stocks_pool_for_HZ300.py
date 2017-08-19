# -*- coding:utf-8 -*-
import pandas as pd
#from WindPy import *
import MySQLdb
from sqlalchemy import create_engine
import datetime,time
import os
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

def getCurrentTime():
    # 获取当前时间
    return time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time()))


def get_stockcode_from_table(table_name):
    db = MySQLdb.connect(host='127.0.0.1', user='root', passwd='zjz4818774', db='investv2', port=3306,charset='utf8')
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    stockcode_list = []

    sql = "SELECT hz300_code FROM "+table_name+" ORDER BY hz300_code ASC"
    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
        for row in rows: 

            stockcode_list.append(row[0])

    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        return ""

    # 关闭数据库连接
    cursor.close()
    db.close()
    return stockcode_list

def get_hz300_stocks_pool():

    engine = create_engine('mysql://root:zjz4818774@localhost/investv2?charset=utf8')
    # print engine
    w.start()
    
    # w.wset("SectorConstituent",u"date=20130608;sector=全部A股")#取全部A股股票代码、名称信息
    #取沪深300指数中股票代码和权重
    hz300_stocks = w.wset("IndexConstituent","date=20170601;windcode=000300.SH;field=wind_code,sec_name,i_weight")
    #取停牌信息
    # w.wset("TradeSuspend","startdate=20150101;enddate=20130608;field=wind_code,sec_name,suspend_type,suspend_reason")
    print hz300_stocks
    hz300_stocks_data = pd.DataFrame()
    hz300_stocks_data['hz300_code']=hz300_stocks.Data[0]
    hz300_stocks_data['sec_name']=hz300_stocks.Data[1]
    hz300_stocks_data['i_weight']=hz300_stocks.Data[2]
    hz300_table_name = 'hz300_stocks'
    try:
        hz300_stocks_data.to_sql(hz300_table_name,engine,if_exists='replace')
    except Exception as e:
        #如果写入数据库失败，写入日志表，便于后续分析处理
        error_log=pd.DataFrame()
        error_log['table']=hz300_table_name
        error_log['error_info']=e
        error_log['created_date']=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        error_log.to_sql('stock_error_log',engine,if_exists='replace')
        print   getCurrentTime(),": SQL Exception :%s" % (e)

'''
    #取ST股票等风险警示股票信息
    ST_stocks = w.wset("SectorConstituent",u"date=20170601;sector=风险警示股票;field=wind_code,sec_name")

    ST_stocks_data = pd.DataFrame()
    ST_stocks_data['ST_code']=ST_stocks.Data[0]
    ST_stocks_data['sec_name']=ST_stocks.Data[1]
    ST_stocks_table_name = 'st_stocks'
    try:
        ST_stocks_data.to_sql(ST_stocks_table_name,engine,if_exists='replace');
    except Exception as e:
        #如果写入数据库失败，写入日志表，便于后续分析处理
        error_log=pd.DataFrame()
        error_log['table']=ST_stocks_table_name
        error_log['error_info']=e
        error_log['created_date']=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        error_log.to_sql('stock_error_log',engine,if_exists='append')
        print   getCurrentTime(),": SQL Exception :%s" % (e)
'''
def get_ipo_date(stock_code):
    w.start()
    result = w.wsd(stock_code, "ipo_date", "2017-06-01", "2017-06-01", "")
    ipo_date = result.Data[0][0]
    return ipo_date

def add_ipo_date_column(table_name):
    db = MySQLdb.connect(host='127.0.0.1', user='root', passwd='zjz4818774', db='investv2', port=3306,charset='utf8')
    cursor = db.cursor()
    sql = "alter table "+table_name+" add ipo_date datetime"
    try:
        cursor.execute(sql)
        # print "add key:ipo_date successfully"
    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        print "but this error will not cause wrong data, everything is ok"
    cursor.close()
    db.close()

def store_ipo_date_into_table(table_name,stock_code):
    db = MySQLdb.connect(host='127.0.0.1', user='root', passwd='zjz4818774', db='investv2', port=3306,charset='utf8')
    cursor = db.cursor()

    ipo_date =str(get_ipo_date(stock_code)).split()[0]
    # ipo_date =str(get_ipo_date(stock_code))

    # print ipo_date

    sql = "UPDATE "+table_name+" SET ipo_date='"+ipo_date+"' WHERE hz300_code='"+stock_code+"'"
    # print sql
    try:
        cursor.execute(sql)
    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
    db.commit()
    # print "store ipo_date successfully"
    cursor.close()
    db.close()

def store_ipodate_column_into_table(table_name):
    #添加一列ipo_date到数据表中
    add_ipo_date_column(table_name)
    #获取股票代码
    stockcode_list = get_stockcode_from_table(table_name)
    # 逐个获取ipo_date,然后存入数据表中
    for stock_code in stockcode_list:

        store_ipo_date_into_table(table_name,stock_code)


def get_stockcode_secname_ipodate_from_table(table_name):
    db = MySQLdb.connect(host='127.0.0.1', user='root', passwd='zjz4818774', db='investv2', port=3306,charset='utf8')
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    stockcode_list = []
    secname_list = []
    ipodate_list = []

    sql = "SELECT hz300_code,sec_name,ipo_date FROM "+table_name+" ORDER BY hz300_code ASC"
    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
        for row in rows: 

            stockcode_list.append(row[0])
            secname_list.append(row[1])
            ipodate_list.append(row[2])

    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        return ""

    # 关闭数据库连接
    cursor.close()
    db.close()
    return stockcode_list,secname_list,ipodate_list

def this_ipo_date_should_be_deleted(ipo_date,given_date):

    # get the seconds for specify date

    ipo_date_time = time.mktime(time.strptime(ipo_date,'%Y-%m-%d %H:%M:%S'))

    given_date_time = time.mktime(time.strptime(given_date,'%Y-%m-%d %H:%M:%S'))
   
    # print "ipo_date_time   is :",ipo_date_time
    # print "given_date_time is :",given_date_time

    if float(ipo_date_time) > float(given_date_time):

        return True
    else:
        return False


def hz300_stocks_ipo_date_delete(table_name):
    # table_name = "hz300_stocks"
    stockcode_list,secname_list,ipodate_list = get_stockcode_secname_ipodate_from_table(table_name)

    # print len(stockcode_list)
    # print len(set(stockcode_list))

    stocks_need_delete_list = []

    delete_stock_index_list = []

    # ipo_date = "2007-12-31 00:00:00"
    # given_date= "2007-12-31 00:00:00"

    # print this_ipo_date_should_be_deleted(ipo_date,given_date)
    # 
    given_date= "2015-12-31 00:00:00"

##########################这是一个错误的思路，没有考虑到ipo_date有重复情况######################################
    # for ipodate in ipodate_list:

    #     if this_ipo_date_should_be_deleted(str(ipodate),given_date):
    #         index = ipodate_list.index(ipodate)
    #         delete_stock_index_list.append(index)
    #         # print ipodate
    #         # print index
    #         # print delete_stock_index_list
    # print "len(delete_stock_index_list)",len(delete_stock_index_list)
    # for index in delete_stock_index_list:
    #     stocks_need_delete_list.append(stockcode_list[index])
###################################################################################################################

    for i in range(len(stockcode_list)):

        if this_ipo_date_should_be_deleted(str(ipodate_list[i]),given_date):
            stocks_need_delete_list.append(stockcode_list[i])


    # print [val for val in list(set(stocks_need_delete_list)) if stocks_need_delete_list.count(val) > 1 ]

    # print "len(stocks_need_delete_list)",len(stocks_need_delete_list)
    

    return stocks_need_delete_list


def hz300_stocks_ST_delete(table_name):
    table_name = "hz300_stocks"
    stockcode_list,secname_list,ipodate_list = get_stockcode_secname_ipodate_from_table(table_name)

    stocks_need_delete_list = []

    delete_stock_index_list = []

    for secname in secname_list:

        if "ST" in secname:
            index = secname_list.index(secname)
            delete_stock_index_list.append(index)
            print secname
            print index
            print delete_stock_index_list

    for item in delete_stock_index_list:
        stocks_need_delete_list.append(stockcode_list[item])
    
    return stocks_need_delete_list

def select_good_stocks():
    table_name = "hz300_stocks"

    stockcode_list,secname_list,ipodate_list = get_stockcode_secname_ipodate_from_table(table_name)
    print stockcode_list
    print "len(stockcode_list) is :",len(stockcode_list)

    stocks_ST_delete_list = hz300_stocks_ST_delete(table_name)
    print stocks_ST_delete_list
    print "len(stocks_ST_delete_list) is :",len(stocks_ST_delete_list)

    stocks_IPO_Date_delete_list = hz300_stocks_ipo_date_delete(table_name)
    print stocks_IPO_Date_delete_list
    print "len(stocks_IPO_Date_delete_list) is :",len(stocks_IPO_Date_delete_list)


    stockcode_list = list(set(stockcode_list).difference(set(stocks_IPO_Date_delete_list))) 
    print "len(stockcode_list) is :",len(stockcode_list)


    stockcode_list = list(set(stockcode_list).difference(set(stocks_ST_delete_list))) 
    print "len(stockcode_list) is :",len(stockcode_list)

    return stockcode_list

if __name__ == "__main__":
    
    # print len(select_good_stocks())

    # get_hz300_stocks_pool()
    # 
    # 
    # ipo_date = "2007-12-31 00:00:00"
    # given_date= "2007-12-31 00:00:00"

    # print this_ipo_date_should_be_deleted(ipo_date,given_date)
    # 

    # hz300_stocks_ipo_date_delete(table_name = "hz300_stocks")
    # 
    # 
    # 
    # 
    get_hz300_stocks_pool()
    
    store_ipodate_column_into_table(table_name="hz300_stocks")

    stock_pool = select_good_stocks()
    print stock_pool
    print len(stock_pool)
