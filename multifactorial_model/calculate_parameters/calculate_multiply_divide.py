#coding=utf-8
import sys
import MySQLdb
import datetime

from deal_with_day_data import *

reload(sys)
sys.setdefaultencoding('utf8')


def insert_ratio_into_table(table_name,insert_column,stock,date,data):
    db = MySQLdb.connect(host='127.0.0.1', user='root', passwd='zjz4818774', db='invest_latest', port=3306,charset='utf8')

    cursor = db.cursor()
    
    try:
        if data != None:
            sql = "UPDATE "+table_name+" SET "+insert_column+"="+str(data)+" WHERE trade_date='"+date+"' AND stock_code='"+stock+"'"
            print sql
            cursor.execute(sql)
            db.commit()
            print "update data successfully"
        else:
            print "data is None, we do not need update it"
    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        print "but this error will not cause wrong data, everything is ok"

    cursor.close()
    db.close()

def get_date_from_table(table_name,stock_code):
    db = MySQLdb.connect(host='127.0.0.1', user='root', passwd='zjz4818774', db='invest_latest', port=3306,charset='utf8')
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    date_list = []

    sql = "SELECT trade_date FROM "+table_name+" WHERE stock_code = '"+stock_code+"' ORDER BY trade_date ASC"
    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
        for row in rows: 
            date = str(row[0])
            date_list.append(date)
            
        # print date_list

    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        return ""

    # 关闭数据库连接
    cursor.close()
    db.close()
    return date_list


def calculate_ratio_function(table_name,stock_code,date,one_column,two_column,three_column,insert_column):
    db = MySQLdb.connect(host='127.0.0.1', user='root', passwd='zjz4818774', db='invest_latest', port=3306,charset='utf8')
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    # date = '2010-06-30 00:00:00'
    sql =  "SELECT "+one_column+","+two_column+","+three_column+" FROM "+table_name+" WHERE stock_code='"+stock_code+"' AND trade_date = '"+date+"'"
    # SELECT wgsd_assets,wgsd_com_eq_paholder FROM table_3m_data WHERE stock_code = '000002.SZ' AND trade_date = '2010-01-01 00:00:00'
    # print sql
    try:
        cursor.execute(sql)
        row = cursor.fetchone()

        if row[0] == None or row[1] == None or row[2] == None:
            result = None
        else:
            temp = float(row[0])*row[1]
            result = temp/row[2]
        print result
    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        return ""
    
    insert_ratio_into_table(table_name,insert_column,stock_code,date,result)
    # 关闭数据库连接
    cursor.close()
    db.close()
    return result


def calculate_store_ratio(symbols,table_name,one_column,two_column,three_column,insert_column):
    db = MySQLdb.connect(host='127.0.0.1', user='root', passwd='zjz4818774', db='invest_latest', port=3306,charset='utf8')
    cursor = db.cursor()
    sql = "alter table "+table_name+" add "+insert_column+" double"
    try:
        cursor.execute(sql)
        print "add key:"+insert_column+" successfully"
    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        print "but this error will not cause wrong data, everything is ok"
    cursor.close()
    db.close()

    for stock_code in symbols:
        date_list = get_date_from_table(table_name,stock_code)
        for date in date_list:
            calculate_ratio_function(table_name,stock_code,date,one_column,two_column,three_column,insert_column)

if __name__ == "__main__":
    
    # symbols=['000002.SZ','000008.SZ','000009.SZ','000060.SZ','000063.SZ','000069.SZ','000100.SZ','000156.SZ','000157.SZ']#通过直接赋值获取股票代码用于测试

    from stocks_pool_for_HZ300 import select_good_stocks

    symbols = select_good_stocks()

    # table_name = "table_3m_data"

    table_name = "table_month_data"
    one_column = "longdebttodebt"
    two_column = "tot_liab"
    three_column = "wgsd_com_eq_paholder"
    insert_column = "longdebt_to_com_eq_paholder"
    calculate_store_ratio(symbols,table_name,one_column,two_column,three_column,insert_column)
    
