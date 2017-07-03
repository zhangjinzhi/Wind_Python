# -*- coding: UTF-8 -*-
import MySQLdb
import sys
import collections

def insert_table_function(cursor,table_name,*key_list,**key_value_dict):
    # db =MySQLdb.connect(host='127.0.0.1',user='root',passwd='zjz4818774',db='invest',port=3306,charset='utf8')
    
    # 使用cursor()方法获取操作游标
    # cursor = db.cursor()

    # SQL 插入数据的语句
    sql = "INSERT INTO `"+table_name+"` VALUES ("
    for key in key_list:
        # print key
        sql += "'"+str(key_value_dict[key])+"'"+","
    #去除最后一个多余的逗号
    sql = sql[0:-1]
    sql += ");"
    
    print sql
    try:
        cursor.execute(sql)
        db.commit()
    except MySQLdb.Error,e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        return "Insert Data in MySQL : "+"Mysql Error %d: %s" % (e.args[0], e.args[1])

    return


if __name__ == "__main__":
    # 打开数据库连接
    db =MySQLdb.connect(host='127.0.0.1',user='root',passwd='zjz4818774',db='invest',port=3306,charset='utf8')
    
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()

    table_name = "temp_table"
    insert_data_dict = collections.OrderedDict()
    insert_data_dict['trade_date']='2017-12-12'
    insert_data_dict['stock_code'] ='600400.SH'
    insert_data_dict['wgsd_com_eq']=10001.43
    insert_data_dict['trade_status']='交易'
    insert_data_dict['created_date']='2017-01-01'
    # print insert_data_dict
    key_list = []
    for key, value in insert_data_dict.items():
        key_list.append(key)
    # print key_list
    sql = insert_table_function(cursor,table_name,*key_list,**insert_data_dict)

    # 关闭数据库连接
    cursor.close()
    db.close()
    # print sql
