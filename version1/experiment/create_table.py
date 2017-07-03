# -*- coding: UTF-8 -*-
import MySQLdb
import sys

def create_table_function(table_name):
	# 打开数据库连接
    db =MySQLdb.connect(host='127.0.0.1',user='root',passwd='zjz4818774',db='invest',port=3306,charset='utf8')

    # 使用cursor()方法获取操作游标
    cursor = db.cursor()

    # SQL 创建表的语句
    sql = "CREATE TABLE `"+table_name+"` (`trade_date` date NOT NULL, `stock_code` text NOT NULL,`wgsd_com_eq` double NOT NULL,`trade_status` text NOT NULL,`created_date` text NOT NULL,PRIMARY KEY (`trade_date`,`stock_code`(10))) ENGINE=InnoDB DEFAULT CHARSET=utf8;"

    try:
        cursor.execute(sql)
 
    except MySQLdb.Error,e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        return 'this Database exists!'

    # 关闭数据库连接
    db.close()
    return


if __name__ == "__main__":
	table_name = "temp_table"
	create_table_function(table_name)
