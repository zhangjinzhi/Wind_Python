#coding=utf-8
import sys
import MySQLdb
reload(sys)
sys.setdefaultencoding('utf8')


def get_one_month_return(stock_code,table_name):
    db = MySQLdb.connect(host='127.0.0.1', user='root', passwd='zjz4818774', db='invest', port=3306,charset='utf8')
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()

    sql = 'select '+stock_code+' from '+table_name
    try:
        cursor.execute(sql)

        row = cursor.fetchone()

        result = row[0]
        print result

    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        return "code : "+str(stock_code)

    # 关闭数据库连接
    db.close()
    return result


if __name__ == "__main__":

    stock_code = "000002.SZ"
    table_name = "table_day_data"
    one_month_return = get_one_month_return(stock_code,table_name)
    print one_month_return
