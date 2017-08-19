#coding=utf-8
import sys
import MySQLdb
import datetime
import pandas as pd

reload(sys)
sys.setdefaultencoding('utf8')

def add_column_into_table(table_name,insert_column):
    db = MySQLdb.connect(host='127.0.0.1', user='root', passwd='zjz4818774', db='invest_latest', port=3306,charset='utf8')
    cursor = db.cursor()
    sql = "alter table "+table_name+" add "+insert_column+" text"
    try:
        cursor.execute(sql)
        print "add column:"+insert_column+" successfully"
    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        print "but this error will not cause wrong data, everything is ok"
    cursor.close()
    db.close()

def insert_industry_into_table(table_name,insert_column,stock_code,industry):
    db = MySQLdb.connect(host='127.0.0.1', user='root', passwd='zjz4818774', db='invest_latest', port=3306,charset='utf8')
    cursor = db.cursor()
    
    try:
        if industry != None and industry != '' and pd.isnull(industry) == False:
            sql = "UPDATE "+table_name+" SET "+insert_column+"='"+industry+"' WHERE stock_code='"+stock_code+"'"
            print sql
            cursor.execute(sql)
            db.commit()
            print "update industry successfully"
        else:
            print "industry is None, we do not need update it"
            print "but this error will not cause wrong data, everything is ok"
    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])

    cursor.close()
    db.close()

def get_industry_from_table(table_name,stock_code):
    db = MySQLdb.connect(host='127.0.0.1', user='root', passwd='zjz4818774', db='invest_latest', port=3306,charset='utf8')
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    date_list = []

    sql = "SELECT industry_citic FROM "+table_name+" WHERE hz300_code = '"+stock_code+"'"
    try:
        cursor.execute(sql)
        row = cursor.fetchone()
        industry = str(row[0])
        print industry 
        # print date_list
    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        return ""

    # 关闭数据库连接
    cursor.close()
    db.close()

    return industry

def get_add_insert_industry(table_name):

    import sys
    sys.path.append('../')
  
    from stocks_pool_for_HZ300 import select_good_stocks

    symbols = select_good_stocks()

    from_table_name = "hz300_stocks"

    to_table_name,insert_column = table_name,"industry_type"

    add_column_into_table(to_table_name,insert_column)

    for stock_code in symbols:

        industry = get_industry_from_table(from_table_name,stock_code)

        insert_industry_into_table(to_table_name,insert_column,stock_code,industry)

def add_dummy_avariable(table_name):

    conn = MySQLdb.connect(host='127.0.0.1', user='root', passwd='zjz4818774', db='invest_latest', port=3306,charset='utf8')

    sql = "SELECT * FROM "+table_name
    try:
        df = pd.read_sql(sql,conn)

    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])


    dummy_df = pd.get_dummies(df['industry_type'],prefix=u'industry_type')


    # dummy_df=df['trade_date'].join(dummy_df.ix[::])
    dummy_df=dummy_df.join(df['trade_date'])
    dummy_df=dummy_df.join(df['stock_code'])

####使用to_sql由于大量数据insert到数据库中，导致error：2006 mysql has gone away，所以暂时不存到数据库中###########
####暂时将运算过程和结果全在内存中进行操作#######

    #去除了'industry_type'这一列
    # original_column = df.columns.values[:-1]
    # include_dummy_df=df[original_column].join(dummy_df.ix[::])
    # print dummy_df
    # print include_dummy_df.columns.values
    ##############################################################################################
    # from sqlalchemy import create_engine
    # engine = create_engine('mysql://root:zjz4818774@127.0.0.1/invest_lastest?charset=utf8')
    # include_dummy_df.to_sql("all_cleaned_data_include_dummy",engine,if_exists='replace')
    #################################################################################################
    return dummy_df

if __name__ == "__main__":

    get_add_insert_industry('table_month_data')
    
    #暂时不用这个函数，该函数被auto_regression中的get_dummy()调用
    # add_dummy_avariable(table_name="all_stocks_cleaned_factors_table")
    print ""
