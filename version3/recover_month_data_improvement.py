#coding=utf-8
import pandas
import sys
import MySQLdb
import datetime
import pandas as pd
reload(sys)
sys.setdefaultencoding('utf8')

from deal_with_day_data import get_all_BOM_EOM



def get_all_December31_Jan31_FebEnd_many_years():
    all_first_day_of_month_many_years,all_end_day_of_month_many_years = get_all_BOM_EOM()
    
    # print all_end_day_of_month_many_years

    all_December31_many_years = []
    all_Jan31_many_years = []
    all_FebEnd_many_years = []

    for end_day_of_month in all_end_day_of_month_many_years:
    	if "12-31" in end_day_of_month:
    		all_December31_many_years.append(end_day_of_month)

    	if "1-31" in end_day_of_month:
    		all_Jan31_many_years.append(end_day_of_month)

    	if "2-28" in end_day_of_month or "2-29" in end_day_of_month:
    		all_FebEnd_many_years.append(end_day_of_month)

    return all_December31_many_years,all_Jan31_many_years,all_FebEnd_many_years


def get_recover_data_function(table_name,stock_code,date_has_data,date_no_data1,date_no_data2,recover_column_list,db,cursor):

    sql = "SELECT "
    
    for recover_column in recover_column_list:
        # print key
        sql += recover_column+","
    #去除最后一个多余的逗号
    sql = sql[0:-1]
    sql += " FROM "+table_name+" WHERE stock_code='"+stock_code+"' AND trade_date = '"+date_has_data+"'"
    print sql
    try:
        cursor.execute(sql)
        row = cursor.fetchone()
        data_list = row

        # print data_list

    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        return ""

    for data in data_list:
        try:
            if data != None and pd.isnull(data) == False:
                sql1 = "UPDATE "+table_name+" SET "+recover_column+"="+str(data)+" WHERE trade_date='"+date_no_data1+"' AND stock_code='"+stock_code+"'"
                # print sql1
                cursor.execute(sql1)
    
                sql2 = "UPDATE "+table_name+" SET "+recover_column+"="+str(data)+" WHERE trade_date='"+date_no_data2+"' AND stock_code='"+stock_code+"'"
                # print sql2
                cursor.execute(sql2)

                # db.commit()
                # print "update data successfully"
            else:
                print "data is None, we do not need update it"
        except MySQLdb.Error, e:
            print "Mysql Error %d: %s" % (e.args[0], e.args[1])
            print "but this error will not cause wrong data, everything is ok"

    db.commit()
    print "update data successfully"

def control_get_recover(symbols,table_name,recover_column_list):

    db = MySQLdb.connect(host='127.0.0.1', user='root', passwd='zjz4818774', db='investv2', port=3306,charset='utf8')
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()

    all_December31_many_years,all_Jan31_many_years,all_FebEnd_many_years = get_all_December31_Jan31_FebEnd_many_years()

    date_has_data_list = all_December31_many_years[:-1]
    date_no_data_list1 = all_Jan31_many_years[1:]
    date_no_data_list2 = all_FebEnd_many_years[1:]

    print date_has_data_list
    print date_no_data_list1
    print date_no_data_list2

    for stock_code in symbols:
    	# for recover_column in recover_column_list:
            for i in range(len(date_has_data_list)):
                get_recover_data_function(table_name,stock_code,date_has_data_list[i],date_no_data_list1[i],date_no_data_list2[i],recover_column_list,db,cursor)


    cursor.close()
    db.close()


if __name__ == "__main__":

    # all_December31_many_years,all_Jan31_many_years,all_FebEnd_many_years = get_all_December31_Jan31_FebEnd_many_years()
    

    from stocks_pool_for_HZ300 import select_good_stocks

    symbols = select_good_stocks()

    table_name = "table_month_data"

    recover_column_list = ["profit_ttm","bps",'fcff','cfps_ttm','wgsd_oper_cf','wgsd_com_eq','wgsd_assets','yoy_or','qfa_yoysales','yoyprofit','qfa_yoyprofit','grossprofitmargin','roe_ttm2','roa_ttm2','yoybps','yoy_assets','wgsd_yoyocf','roa','roe','grossprofitmargin_ttm2','assetsturn','faturn','op_ttm2','current','cashtocurrentdebt','quick','wgsd_com_eq_paholder','longdebttodebt','tot_liab','debttoassets']
    print len(recover_column_list)
    control_get_recover(symbols,table_name,recover_column_list)


