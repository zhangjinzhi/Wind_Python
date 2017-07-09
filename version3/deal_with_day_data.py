#coding=utf-8
import pandas
import sys
import MySQLdb
import datetime
reload(sys)
sys.setdefaultencoding('utf8')

def get_first_end_day_of_month(input_year):

    start_day_of_month_list = []
    end_day_of_month_list = []

    for x in xrange(1, 13):
      start_day_of_month = (datetime.datetime(input_year, x, 1)).strftime("%Y-%m-%d")
      if 12 == x:
        end_day_of_month = (datetime.datetime(input_year, 12, 31)).strftime("%Y-%m-%d")
      else:
        end_day_of_month = (datetime.datetime(input_year, x+1, 1) - datetime.timedelta(days = 1)).strftime("%Y-%m-%d")
      # print start_day_of_month, end_day_of_month
      
      start_day_of_month_list.append(start_day_of_month)
      end_day_of_month_list.append(end_day_of_month)

    return start_day_of_month_list,end_day_of_month_list


def get_all_BOM_EOM():
    we_select_years = range(2008,2017)
    # print we_select_years
    all_first_day_of_month_many_years = []
    all_end_day_of_month_many_years = []

    for year in we_select_years:
        first_day_of_month,end_day_of_month= get_first_end_day_of_month(year)
        all_first_day_of_month_many_years += first_day_of_month
        all_end_day_of_month_many_years += end_day_of_month


        
    # print all_end_day_of_month_many_years

    return all_first_day_of_month_many_years,all_end_day_of_month_many_years

def get_date_close_list(stock_code,table_name):
    db = MySQLdb.connect(host='127.0.0.1', user='root', passwd='zjz4818774', db='investv2', port=3306,charset='utf8')
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()

    sql =  "SELECT table_day_data.close,table_day_data.trade_date FROM table_day_data WHERE stock_code='"+stock_code+"' ORDER BY table_day_data.trade_date ASC"
    try:
        cursor.execute(sql)
        close_price_list = []
        date_list = []
        rows = cursor.fetchall()
        for row in rows:

            close_price_list.append(row[0])
            date_list.append(row[1].strftime("%Y-%m-%d"))

        # print len(close_price_list)
        # print len(date_list)
        # print date_list

        result = ''
    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        return "stock_code : "+str(stock_code)

    # 关闭数据库连接
    db.close()
    return close_price_list,date_list





if __name__ == "__main__":
    print ""
    
























































'''


############################################计算动量因子#########################################################

def calculate_one_month_return():
    # one
    stock_code = "000002.SZ"
    table_name = "table_day_data"
    all_close_price_list,all_date_list = get_date_close_list(stock_code,table_name)
    all_BOM,all_EOM = get_all_BOM_EOM()
    index_list = []
    for i in range(len(all_EOM)):

        index = all_date_list.index(all_EOM[i])
        index_list.append(index)
    print index_list
    for i in range(len(index_list)-1):
        print i
        print index_list[i]
        print float((all_close_price_list[index_list[i+1]] - all_close_price_list[index_list[i]]))/all_close_price_list[index_list[i]]

def calculate_three_month_return():
    # one
    stock_code = "000002.SZ"
    table_name = "table_day_data"
    all_close_price_list,all_date_list = get_date_close_list(stock_code,table_name)
    all_BOM,all_EOM = get_all_BOM_EOM()
    index_list = []
    for i in range(len(all_EOM)):

        index = all_date_list.index(all_EOM[i])
        index_list.append(index)
    print index_list
    for i in range(len(index_list)-3):
        print i
        print index_list[i]
        print float((all_close_price_list[index_list[i+3]] - all_close_price_list[index_list[i]]))/all_close_price_list[index_list[i]]

def calculate_six_month_return():
    # one
    stock_code = "000002.SZ"
    table_name = "table_day_data"
    all_close_price_list,all_date_list = get_date_close_list(stock_code,table_name)
    all_BOM,all_EOM = get_all_BOM_EOM()
    index_list = []
    for i in range(len(all_EOM)):

        index = all_date_list.index(all_EOM[i])
        index_list.append(index)
    print index_list
    for i in range(len(index_list)-6):
        print i
        print index_list[i]
        print float((all_close_price_list[index_list[i+6]] - all_close_price_list[index_list[i]]))/all_close_price_list[index_list[i]]

def calculate_one_year_return():
    # one
    stock_code = "000002.SZ"
    table_name = "table_day_data"
    all_close_price_list,all_date_list = get_date_close_list(stock_code,table_name)
    all_BOM,all_EOM = get_all_BOM_EOM()
    index_list = []
    for i in range(len(all_EOM)):

        index = all_date_list.index(all_EOM[i])
        index_list.append(index)
    print index_list
    for i in range(len(index_list)-12):
        print i
        print index_list[i]
        print float((all_close_price_list[index_list[i+12]] - all_close_price_list[index_list[i]]))/all_close_price_list[index_list[i]]

def calculate_two_years_return():
    # one
    stock_code = "000002.SZ"
    table_name = "table_day_data"
    all_close_price_list,all_date_list = get_date_close_list(stock_code,table_name)
    all_BOM,all_EOM = get_all_BOM_EOM()
    index_list = []
    for i in range(len(all_EOM)):

        index = all_date_list.index(all_EOM[i])
        index_list.append(index)
    print index_list
    for i in range(len(index_list)-24):
        print i
        print index_list[i]
        print float((all_close_price_list[index_list[i+24]] - all_close_price_list[index_list[i]]))/all_close_price_list[index_list[i]]

def calculate_highest_one_day_return_of_month():
    highest_one_day_return_list = []
    stock_code = "000002.SZ"
    table_name = "table_day_data"
    all_close_price_list,all_date_list = get_date_close_list(stock_code,table_name)

    #计算2010到2016每一天的日收益率
    one_day_return_list = [0]
    for i in range(len(all_close_price_list)-1):
        one_day_return = float(all_close_price_list[i+1]-all_close_price_list[i])/all_close_price_list[i]
        one_day_return_list.append(one_day_return)
    print "one_day_return_list的长度是: ",len(one_day_return_list)

    #获取2010到2016的月初日期的列表和月末日期的列表
    all_BOM,all_EOM = get_all_BOM_EOM()
    
    BOM_index_list = []
    EOM_index_list = []

    for i  in range(len(all_BOM)):
        BOM_index_list.append(all_date_list.index(all_BOM[i]))
    print BOM_index_list

    # if len(all_BOM)==len(BOM_index_list):
        # print "len(all_BOM)==len(BOM_index_list)"


    for i  in range(len(all_EOM)):
        EOM_index_list.append(all_date_list.index(all_EOM[i]))
    print EOM_index_list

 
    if len(BOM_index_list)==len(EOM_index_list) and len(one_day_return_list)==len(all_date_list):
        print "len(BOM_index_list)==len(EOM_index_list)"

        for i in range(len(BOM_index_list)):

            highest_one_day_return = max(one_day_return_list[BOM_index_list[i]:(EOM_index_list[i]+1)])
            # print all_date_list[BOM_index_list[i]:(EOM_index_list[i]+1)]
            highest_one_day_return_list.append(highest_one_day_return)
            

    return highest_one_day_return_list

    else:
        print "len(BOM_index_list)不等于len(EOM_index_list)，错误"
        return


############################################计算动波动因子#########################################################

def calculate_highest_to_lowest_of_one_month():
    highest_to_lowest_list = [0]
    stock_code = "000002.SZ"
    table_name = "table_day_data"
    all_close_price_list,all_date_list = get_date_close_list(stock_code,table_name)

    #获取2010到2016的月初日期的列表和月末日期的列表
    all_BOM,all_EOM = get_all_BOM_EOM()
    
    BOM_index_list = []
    EOM_index_list = []

    for i  in range(len(all_BOM)):
        BOM_index_list.append(all_date_list.index(all_BOM[i]))
    print BOM_index_list

    # if len(all_BOM)==len(BOM_index_list):
        # print "len(all_BOM)==len(BOM_index_list)"

    for i  in range(len(all_EOM)):
        EOM_index_list.append(all_date_list.index(all_EOM[i]))
    print EOM_index_list

 
    if len(BOM_index_list)==len(EOM_index_list):
        print "len(BOM_index_list)==len(EOM_index_list)"

        for i in range(len(BOM_index_list)):

            max_daily_stock_price = max(all_close_price_list[BOM_index_list[i]:(EOM_index_list[i]+1)])
            # print all_date_list[BOM_index_list[i]:(EOM_index_list[i]+1)]
            min_daily_stock_price = min(all_close_price_list[BOM_index_list[i]:(EOM_index_list[i]+1)])
            
            highest_to_lowest = max_daily_stock_price/min_daily_stock_price

            highest_to_lowest_list.append(highest_to_lowest)
            

    return highest_to_lowest_list

    else:
        print ""
        return

def calculate_highest_to_lowest_of_three_month():
    highest_to_lowest_of_three_month_list = [0，0]
    stock_code = "000002.SZ"
    table_name = "table_day_data"
    all_close_price_list,all_date_list = get_date_close_list(stock_code,table_name)

    #获取2010到2016的月初日期的列表和月末日期的列表
    all_BOM,all_EOM = get_all_BOM_EOM()
    
    BOM_index_list = []
    EOM_index_list = []

    for i  in range(len(all_BOM)):
        BOM_index_list.append(all_date_list.index(all_BOM[i]))
    print BOM_index_list

    # if len(all_BOM)==len(BOM_index_list):
        # print "len(all_BOM)==len(BOM_index_list)"

    for i  in range(len(all_EOM)):
        EOM_index_list.append(all_date_list.index(all_EOM[i]))
    print EOM_index_list

 
    if len(BOM_index_list)==len(EOM_index_list):
        print "len(BOM_index_list)==len(EOM_index_list)"

        for i in range(len(BOM_index_list)-2):
            # EOM_index 里面在all_close_price_list和all_date_list中的位置,EOM_index_list[i]中的i是EOM_index_list中的位置
            # EOM_index_list[i+2]+1 加1是因为切片是"取头不取尾"
            max_three_month_stock_price = max(all_close_price_list[BOM_index_list[i]:(EOM_index_list[i+2]+1)])
            # print all_date_list[BOM_index_list[i]:(EOM_index_list[i]+1)]
            min_three_month_stock_price = min(all_close_price_list[BOM_index_list[i]:(EOM_index_list[i+2]+1)])
            
            highest_to_lowest = max_three_month_stock_price/min_three_month_stock_price

            highest_to_lowest_of_three_month_list.append(highest_to_lowest)
            

    return highest_to_lowest_of_three_month_list

    else:
        print ""
        return

'''

# if __name__ == "__main__":
    
    

