#coding=utf-8
import sys
import MySQLdb
import datetime


reload(sys)
sys.setdefaultencoding('utf8')

def get_date_turn_list(stock_code,table_name):
    db = MySQLdb.connect(host='127.0.0.1', user='root', passwd='zjz4818774', db='invest_latest', port=3306,charset='utf8')
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()

    sql =  "SELECT turn,trade_date FROM "+table_name+" WHERE stock_code='"+stock_code+"' ORDER BY trade_date ASC"
    try:
        cursor.execute(sql)
        turn_list = []
        date_list = []
        rows = cursor.fetchall()
        for row in rows:
            
            if row[0] == None:
                turn_list.append(0)
            else:
                turn_list.append(row[0])
            date_list.append(row[1].strftime("%Y-%m-%d"))

        # print len(close_price_list)
        # print len(date_list)
        # print date_list

    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        return "stock_code : "+str(stock_code)

    # 关闭数据库连接
    db.close()
    return turn_list,date_list




def insert_data_into_table(table_name,insert_column,stock,date_list,data_list):
    db = MySQLdb.connect(host='127.0.0.1', user='root', passwd='zjz4818774', db='invest_latest', port=3306,charset='utf8')

    cursor = db.cursor()
    
    try:
        if len(date_list)==len(data_list):
            for i in range(len(date_list)):
                if data_list[i] != None:
                    sql = "UPDATE "+table_name+" SET "+insert_column+"="+str(data_list[i])+" WHERE trade_date='"+date_list[i]+" 00:00:00'"+" AND stock_code='"+stock+"'"
                    print sql
                    cursor.execute(sql)
                else:
                    print "data is None ,do not need to update it"
        else:
            print "ERROR: insert data into table...len(date_list)!=len(data_list)"

        db.commit()
        print "update data successfully"
    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        print "but this error will not cause wrong data, everything is ok"

    cursor.close()
    db.close()

##################################### ######计算3、6个月换手率#########################################################
############################################统一封装好的函数###############################

def calculate_month_turn(stock_code,time_length):

    month_turn_list = [0 for i in range(time_length-1) ]
    # stock_code = "000002.SZ"
    table_name = "table_month_data"
    turn_list,date_list = get_date_turn_list(stock_code,table_name)

    for i in range(len(date_list)-(time_length-1)):
        # print i
        # print index_list[i]
        if turn_list[i] == None:
            turn_list[i] = 0
        if turn_list[i+1] == None:
            turn_list[i+1] = 0
        if turn_list[i+2] == None:
            turn_list[i+2] = 0
        
        month_turn = turn_list[i]+turn_list[i+1]+turn_list[i+2]
        month_turn_list.append(month_turn)

    insert_data_into_table("table_month_data",str(time_length)+"_month_turn",stock_code,date_list,month_turn_list)

    return month_turn_list


###############################################控制计算和存储#########################################################
##############################################统一封装好的函数########################################################
def calculate_and_store_month_turn(symbols,time_length): 
    db = MySQLdb.connect(host='127.0.0.1', user='root', passwd='zjz4818774', db='invest_latest', port=3306,charset='utf8')

    cursor = db.cursor()

    sql = "alter table table_month_data add "+str(time_length)+"_month_turn double"

    try:
        cursor.execute(sql)
        print "add key:"+str(time_length)+"_month_turn successfully"
    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        print "but this error will not cause wrong data, everything is ok"
    cursor.close()
    db.close()

    # symbols=['000002.SZ','000008.SZ','000009.SZ','000060.SZ','000063.SZ','000069.SZ','000100.SZ','000156.SZ','000157.SZ']#通过直接赋值获取股票代码用于测试
    for symbol in symbols:

        calculate_month_turn(symbol,time_length)
    


if __name__ == "__main__":
    
    # symbols=['000002.SZ','000008.SZ','000009.SZ','000060.SZ','000063.SZ','000069.SZ','000100.SZ','000156.SZ','000157.SZ']#通过直接赋值获取股票代码用于测试
    from stocks_pool_for_HZ300 import select_good_stocks

    symbols = select_good_stocks()

    calculate_and_store_month_turn(symbols,3)
    calculate_and_store_month_turn(symbols,6)
