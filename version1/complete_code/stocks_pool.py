# -*- coding:utf-8 -*-
import pandas as pd
import MySQLdb
from WindPy import *
from sqlalchemy import create_engine
import datetime,time
import os
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

def getCurrentTime():
    # 获取当前时间
    return time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time()))

def get_stocks_pool():

    engine = create_engine('mysql://root:zjz4818774@localhost/invest?charset=utf8')
    w.start()
    # w.wset("SectorConstituent",u"date=20130608;sector=全部A股")#取全部A股股票代码、名称信息
    #取沪深300指数中股票代码和权重
    HZ300_stocks = w.wset("IndexConstituent","date=20161231;windcode=000300.SH;field=wind_code,sec_name,i_weight")
    #取停牌信息
    # w.wset("TradeSuspend","startdate=20150101;enddate=20130608;field=wind_code,sec_name,suspend_type,suspend_reason")

    HZ300_stocks_data = pd.DataFrame()
    HZ300_stocks_data['HZ300_code']=HZ300_stocks.Data[0]
    HZ300_stocks_data['sec_name']=HZ300_stocks.Data[1]
    HZ300_stocks_data['i_weight']=HZ300_stocks.Data[2]
    HZ300_table_name = 'hz300_stocks'
    try:
        HZ300_stocks_data.to_sql(HZ300_table_name,engine,if_exists='replace')
    except Exception as e:
        #如果写入数据库失败，写入日志表，便于后续分析处理
        error_log=pd.DataFrame()
        error_log['table']=HZ300_table_name
        error_log['error_info']=e
        error_log['created_date']=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        error_log.to_sql('stock_error_log',engine,if_exists='replace')
        print   getCurrentTime(),": SQL Exception :%s" % (e)


    #取ST股票等风险警示股票信息
    ST_stocks = w.wset("SectorConstituent",u"date=20161230;sector=风险警示股票;field=wind_code,sec_name")

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

def get_stockcode_from_table(table_name):
    db = MySQLdb.connect(host='127.0.0.1', user='root', passwd='zjz4818774', db='invest', port=3306,charset='utf8')
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    stockcode_list = []

    sql = "SELECT HZ300_code FROM "+table_name+" ORDER BY HZ300_code ASC"
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


def get_ipo_date(stock_code):
    w.start()
    result = w.wsd(stock_code, "ipo_date", "2017-06-01", "2017-06-01", "")
    ipo_date = result.Data[0][0]
    return ipo_date

def add_ipo_date_column(table_name):
    db = MySQLdb.connect(host='127.0.0.1', user='root', passwd='zjz4818774', db='invest', port=3306,charset='utf8')
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
    db = MySQLdb.connect(host='127.0.0.1', user='root', passwd='zjz4818774', db='invest', port=3306,charset='utf8')
    cursor = db.cursor()

    ipo_date =str(get_ipo_date(stock_code)).split()[0]
    # ipo_date =str(get_ipo_date(stock_code))

    print ipo_date

    sql = "UPDATE "+table_name+" SET ipo_date='"+ipo_date+"' WHERE HZ300_code='"+stock_code+"'"
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


if __name__ == "__main__":
    # get_stocks_pool()
    store_ipodate_column_into_table(table_name="hz300_stocks")
