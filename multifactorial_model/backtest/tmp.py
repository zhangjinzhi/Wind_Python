#coding=utf8
import pandas as pd
import numpy as np
import pandas
import sys
import MySQLdb
import datetime

reload(sys)
sys.setdefaultencoding('utf8')


def pandas_cleaned_data():
    import pymysql
    conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='zjz4818774', db='table_month_data')
    cursor = conn.cursor()
    # cursor.execute("DROP TABLE IF EXISTS test")#必须用cursor才行

    # sql = "select * from table_month_data where stock_code='"+stock_code+"' and trade_date>'"+ipo_date+"' ORDER BY trade_date ASC"
    sql = "select * from final_all_stocks_cleaned_factors_table"
    df = pd.read_sql(sql,conn)
    # print df["industry_type"]
    content = pd.DataFrame(df)
    return content
def pandas_read_month_data():
    import pymysql
    conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='zjz4818774', db='month_data')
    cursor = conn.cursor()
    # cursor.execute("DROP TABLE IF EXISTS test")#必须用cursor才行

    # sql = "select * from table_month_data where stock_code='"+stock_code+"' and trade_date>'"+ipo_date+"' ORDER BY trade_date ASC"
    sql = "select * from table_month_data"
    df = pd.read_sql(sql,conn)
    # print df["industry_type"]
    content = pd.DataFrame(df)
    return content


def pandas_read_hz300():
    import pymysql
    conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='zjz4818774', db='month_data',charset='utf8')
    cursor = conn.cursor()
    sql = "select * from hz300_stocks"
    df = pd.read_sql(sql, conn)
    content = pd.DataFrame(df)
    return content


def output_portfolio_industry(backtest_dict):
    hz300_df = pandas_read_hz300()
    hz300_df = hz300_df.set_index(hz300_df["hz300_code"])
    backtest_df = pd.DataFrame(backtest_dict)
    industry_dcit = dict()
    all_EOM_list = all_EOM()
    for date in map(lambda x: x.split(' ')[0], all_EOM_list[:-1]):
        portfolio = backtest_df[date].tolist()
        industry_list = []
        for stock_code in portfolio:
            industry = hz300_df.ix[stock_code, "industry_citic"]
            industry_list.append(industry)
        industry_dcit[date] = industry_list

    pd.DataFrame(industry_dcit).to_csv("date_industry_30.csv", encoding="utf-8")
    describe =  pd.DataFrame(industry_dcit).describe()
    describe.to_csv("describe_date_industry_30.csv", encoding="utf-8")

def get_first_end_day_of_month(input_year):
    start_day_of_month_list = []
    end_day_of_month_list = []

    for x in xrange(1, 13):
        start_day_of_month = (datetime.datetime(input_year, x, 1)).strftime("%Y-%m-%d %H:%M:%S")
        if 12 == x:
            end_day_of_month = (datetime.datetime(input_year, 12, 31)).strftime("%Y-%m-%d %H:%M:%S")
        else:
            end_day_of_month = (datetime.datetime(input_year, x + 1, 1) - datetime.timedelta(days=1)).strftime(
                "%Y-%m-%d %H:%M:%S")
        # print start_day_of_month, end_day_of_month

        start_day_of_month_list.append(start_day_of_month)
        end_day_of_month_list.append(end_day_of_month)

    return start_day_of_month_list, end_day_of_month_list


def get_all_BOM_EOM():
    we_select_years = range(2010, 2017)
    # print we_select_years
    all_first_day_of_month_many_years = []
    all_end_day_of_month_many_years = []

    for year in we_select_years:
        first_day_of_month, end_day_of_month = get_first_end_day_of_month(year)
        all_first_day_of_month_many_years += first_day_of_month
        all_end_day_of_month_many_years += end_day_of_month

    # print all_end_day_of_month_many_years

    return all_first_day_of_month_many_years, all_end_day_of_month_many_years

def all_EOM():
    all_first_day_of_month_many_years, all_end_day_of_month_many_years = get_all_BOM_EOM()
    return all_end_day_of_month_many_years

import numpy as np
import pandas as pd


def normalization(series):
    return (series - min(series)) / (max(series) - min(series))

def filter_index():
    df = pd.read_csv('output_include_dummy.csv')

    factor_list = set(df["factor"].tolist())
    result_dict = {factor: 0 for factor in factor_list}
    # result_list = []
    for factor in factor_list:
        temp_df = df[df["factor"] == factor]
        temp_df_length = 1.0 * len(temp_df)
        result_dict[factor] = len(temp_df[temp_df["tvalues"] > 2]) / temp_df_length
        # result_list.append(len(temp_df[temp_df["tvalues"]>2])/temp_df_length)
    # print result_dict
    output = dict()
    for item in result_dict:
        if result_dict[item] >= 0.4:
            output[item] = result_dict[item]

    return output


def get_stockset_by_score(Factor,date,filtered_index):
    import datetime
    # Factpandas_cleaned_data()

    # print Factor["trade_date"]
    # print 'Factor["trade_date"][0]',Factor["trade_date"][0],type(Factor["trade_date"][0])
    # print 'pd.to_datetime(date)',pd.to_datetime(date),type(pd.to_datetime(date))
    # print 'Factor["trade_date"][0]==pd.to_datetime(date)',Factor["trade_date"][0]==pd.to_datetime(date)
    Factor = Factor[Factor["trade_date"]==pd.to_datetime(date)]
    # print Factor
    Factor.set_index('stock_code',inplace=True)
    # print Factor
    # Factor_NetProfitGrowRate = Factor['NetProfitGrowRate'].dropna().to_dict()
    Total_Score = pd.DataFrame(index=Factor.index,columns=filtered_index, data=0)
    # print Total_Score
    from sklearn import preprocessing

    for index in filtered_index:
        signal_temp = Factor[index].dropna().to_dict()
        Total_Score[index][signal_temp.keys()] = preprocessing.scale(signal_temp.values())
        # Total_Score[index][signal_temp.keys()] = signal_temp.values()

    # print Total_Score

    # 等权重
    Total_Score['total_score'] = np.dot(Total_Score, np.array([1.0/len(filtered_index) for i in filtered_index]))  # 等权求和评分

    buylist = Total_Score.sort_values('total_score').tail(30).index.tolist()  # 选出评分最高的30只股票
    # buylist = {s: 0 for s in buylist} 这里可以给购买的权重

    return buylist


def get_monthly_return(df,date, buylist):
    # df = pandas_cleaned_data()
    monthly_return_list = []
    monthly_return_df = df[df["trade_date"] == pd.to_datetime(date)]
    monthly_return_df.set_index('stock_code',inplace=True)
    # print monthly_return_df
    for stock in buylist:
        # print monthly_return_df.ix[stock, "1_month_return"]
        if np.isnan(monthly_return_df.ix[stock,"1_month_return"]) == False:
            monthly_return_list.append(monthly_return_df.ix[stock,"1_month_return"])
        else:
            monthly_return_list.append(0)
    return monthly_return_list

def backtest():
    index_dict = filter_index()
    cleaned_data_df = pandas_cleaned_data()
    table_month_data_df = pandas_read_month_data()
    return_list = []
    backtest_dict = dict()
    all_EOM_list = all_EOM()
    for i in range(len(all_EOM())-1):
        date = all_EOM_list[i]
        date_next = all_EOM_list[i+1]
        buylist = get_stockset_by_score(cleaned_data_df,date,index_dict.keys())
        print "日期，股票组合，该日期的收益率"
        print "from ",date," to ",date_next
        print buylist
        #date 是股票调整的日期
        backtest_dict[str(date.split(' ')[0])] = buylist
        monthly_return_list = get_monthly_return(table_month_data_df,date_next, buylist)
        print monthly_return_list
        backtest_dict[str(date_next.split(' ')[0])+"monthly_return_list"] = monthly_return_list
        #每个股票按照等权重求和，得到组合的收益率
        return_list.append(sum(monthly_return_list) / len(monthly_return_list))

    x = map(lambda x: x.split(' ')[0], all_EOM()[:-1])
    print "############################################"
    print "最终每个月末的收益率"
    print return_list
    # print pd.DataFrame(backtest_dict)
    output_portfolio_industry(backtest_dict)
    y = return_list
    # return_list = map(lambda x:x+1,return_list)
    # print return_list
    # y = np.cumsum(return_list)
    print y
    plt_return(x, y)

    return return_list

def plt_return(x, y):
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    import datetime
    # X轴，Y轴数据
    # x = all_EOM()
    # y = backtest(buylist)
    x = [datetime.datetime.strptime(d, '%Y-%m-%d').date() for d in x]
    plt.figure(figsize=(8, 4))  # 创建绘图对象
    # 配置横坐标
    # plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    # plt.gca().xaxis.set_major_locator(mdates.DayLocator())

    plt.plot(x, y, "b--", linewidth=1)  # 在当前绘图对象绘图（X轴，Y轴，蓝色虚线，线宽度）
    plt.xlabel("Date")  # X轴标签
    plt.ylabel("Return")  # Y轴标签
    plt.title("BackTest")  # 图标题
    plt.axhline(0)
    plt.show()  # 显示图
    # plt.savefig("line.jpg")  # 保存图

if __name__ == '__main__':
    # x = map(lambda x: x.split(' ')[0],all_EOM())
    # y = [0.1 for i in x]
    # plt_return(x,y)
    backtest()
    # print pandas_read_hz300()