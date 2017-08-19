#coding=utf-8
import sys
import MySQLdb
import datetime
import pandas as pd

reload(sys)
sys.setdefaultencoding('utf8')

def insert_data_into_table(table_name,insert_column,stock,date_list,data_list):
    db = MySQLdb.connect(host='127.0.0.1', user='root', passwd='zjz4818774', db='invest_latest', port=3306,charset='utf8')

    cursor = db.cursor()
    
    try:
        if len(date_list)==len(data_list):
            for i in range(len(date_list)):
                if pd.isnull(data_list[i])==False and data_list[i]!= None and data_list[i]!= '':
                    sql = "UPDATE "+table_name+" SET "+insert_column+"="+str(data_list[i])+" WHERE trade_date='"+date_list[i]+"' AND stock_code='"+stock+"'"
                    print sql
                    cursor.execute(sql)
                else:
                    #print data_list[i]
                    print "this data is None or NaN, so we don not need to update it"
        else:
            print "ERROR: insert data into table...len(date_list)!=len(data_list)"

        db.commit()
        print "update data successfully"
    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        print "but this error will not cause wrong data, everything is ok"

    cursor.close()
    db.close()


def get_data_date_list_from_table(table_name,stock_code,data_name):
    db = MySQLdb.connect(host='127.0.0.1', user='root', passwd='zjz4818774', db='invest_latest', port=3306,charset='utf8')
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    date_list = []
    data_list = []

    sql = "SELECT trade_date"+","+data_name+" FROM "+table_name+" WHERE stock_code = '"+stock_code+"' ORDER BY trade_date ASC"
    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
        for row in rows: 
            date = str(row[0])
            date_list.append(date)
            data = row[1]
            data_list.append(data)
        # print date_list

    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        return ""

    # 关闭数据库连接
    cursor.close()
    db.close()
    return data_list,date_list


def calculate_GrowthRate_date_list(stock_code,table_name,one_column,insert_column):

    all_data_list,all_date_list = get_data_date_list_from_table(table_name,stock_code,one_column)
    
    temp_1 = all_data_list[0:-1]
    temp_2 = all_data_list[1:]
    # temp_1 = [5,5,5,5,5]
    # temp_2 = [7.5,7.5,7.5,7.5,7.5]
    # print temp_1
    # print temp_2
    temp_GrowthRate_list = []

    for i in range(len(temp_1)):
        #print temp_1[i]
        #print temp_2[i]
        if temp_1[i]!= None and temp_1[i]!=0  and temp_2[i]!= None:

            temp_GrowthRate = (temp_2[i] - temp_1[i]) / temp_1[i]
            
        else:
            temp_GrowthRate = None
        #print "temp_GrowthRate is : ",temp_GrowthRate
        temp_GrowthRate_list.append(temp_GrowthRate)

    # temp_1 = pd.Series(temp_1)
    # temp_2 = pd.Series(temp_2)
    # # 针对两个list中数据都完整时，是可以的。但有数据为None的话，就会计算错误
    # # 所以用pandas处理
    # temp_GrowthRate_list = map(lambda (a,b):(b-a)/a, zip(temp_1,temp_2))

    GrowthRate_list=[0]+temp_GrowthRate_list

    insert_data_into_table(table_name,insert_column,stock_code,all_date_list,GrowthRate_list)

    return GrowthRate_list,all_date_list

###############################################控制计算和存储#########################################################
##############################################统一封装好的函数########################################################
def calculate_store_GrowthRate(symbols,table_name,one_column,insert_column):
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
        calculate_GrowthRate_date_list(stock_code,table_name,one_column,insert_column)


if __name__ == "__main__":
    
    # symbols=['000002.SZ','000008.SZ','000009.SZ','000060.SZ','000063.SZ','000069.SZ','000100.SZ','000156.SZ','000157.SZ']#通过直接赋值获取股票代码用于测试
  
    from stocks_pool_for_HZ300 import select_good_stocks

    symbols = select_good_stocks()  

    #################################################################################
    # table_name = "table_3m_data"
    table_name = "table_month_data"

    one_column = "grossprofitmargin"
    insert_column = "grossprofitmargin_growthrate"
    calculate_store_GrowthRate(symbols,table_name,one_column,insert_column)
    ##################################################################################
    table_name = "table_month_data"
    one_column = "eps_ttm"
    insert_column = "eps_ttm_growthrate"
    calculate_store_GrowthRate(symbols,table_name,one_column,insert_column)
    #################################################################################
    # table_name = "table_3m_data"

    table_name = "table_month_data"
    one_column = "roe_ttm2"
    insert_column = "roe_ttm2_growthrate"
    calculate_store_GrowthRate(symbols,table_name,one_column,insert_column)
    ##################################################################################
    # table_name = "table_3m_data"

    table_name = "table_month_data"
    one_column = "roa_ttm2"
    insert_column = "roa_ttm2_growthrate"
    calculate_store_GrowthRate(symbols,table_name,one_column,insert_column)
    #################################################################################
    
