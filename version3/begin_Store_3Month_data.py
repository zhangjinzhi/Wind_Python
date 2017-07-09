# -*- coding:utf-8 -*-
####################################################################################################################
'''
从wind采集原始数据，存储到mysql中
'''
####################################################################################################################
import pandas as pd
from WindPy import *
from sqlalchemy import create_engine
import datetime,time
import os
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


class WindStock():

    def getCurrentTime(self):
        # 获取当前时间
        return time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time()))

    def AStockHisData(self,symbols,start_date,end_date,step=0):
        '''
        逐个股票代码查询行情数据
        wsd代码可以借助 WindNavigator自动生成copy即可使用;时间参数不设，默认取当前日期，可能是非交易日没数据;
        只有一个时间参数时，默认作为为起始时间，结束时间默认为当前日期；如设置两个时间参数则依次为起止时间
        '''
        print self.getCurrentTime(),": Download A Stock Starting:"
        for symbol in symbols:
             w.start()
             try:
                 #stock=w.wsd(symbol,'trade_code,open,high,low,close,volume,amt',start_date,end_date)
                 '''
                 wsd代码可以借助 WindNavigator自动生成copy即可使用;
                 时间参数不设，默认取当前日期，可能是非交易日没数据;
                 只有一个时间参数，默认为起始时间到最新；如设置两个时间参数则依次为起止时间
                '''
                 table_name = 'table_3M_data'
                 #第一处修改
                 stock=w.wsd(symbol, "profit_ttm,bps,fcff,cfps_ttm,wgsd_oper_cf,wgsd_assets,yoy_or,qfa_yoysales,yoyprofit,qfa_yoyprofit,grossprofitmargin,roe_ttm2,roa_ttm2,yoybps,yoy_assets,wgsd_yoyocf,roa,roe,grossprofitmargin_ttm2,assetsturn,faturn,op_ttm2,current,cashtocurrentdebt,quick,wgsd_com_eq_paholder,longdebttodebt,tot_liab,debttoassets,mkt_cap_ard,wgsd_com_eq,close,trade_status", start_date, end_date, "unit=1;currencyType=;rptType=1;Period=Q;Days=Alldays;PriceAdj=F")

                 index_data = pd.DataFrame()
                 index_data['trade_date']=stock.Times
                 index_data['stock_code'] =symbol
                 index_data['profit_ttm']=stock.Data[0]
                 index_data['bps']=stock.Data[1]
                 index_data['fcff']=stock.Data[2]
                 index_data['cfps_ttm']=stock.Data[3]
                 index_data['wgsd_oper_cf']=stock.Data[4]
                 index_data['wgsd_assets']=stock.Data[5]
                 index_data['yoy_or']=stock.Data[6]
                 index_data['qfa_yoysales']=stock.Data[7]
                 index_data['yoyprofit']=stock.Data[8]
                 index_data['qfa_yoyprofit']=stock.Data[9]
                 index_data['grossprofitmargin']=stock.Data[10]
                 index_data['roe_ttm2']=stock.Data[11]
                 index_data['roa_ttm2']=stock.Data[12]
                 index_data['yoybps']=stock.Data[13]
                 index_data['yoy_assets']=stock.Data[14]
                 index_data['wgsd_yoyocf']=stock.Data[15]
                 index_data['roa']=stock.Data[16]
                 index_data['roe']=stock.Data[17]
                 index_data['grossprofitmargin_ttm2']=stock.Data[18]
                 index_data['assetsturn']=stock.Data[19]
                 index_data['faturn']=stock.Data[20]
                 index_data['op_ttm2']=stock.Data[21]
                 index_data['current']=stock.Data[22]
                 index_data['cashtocurrentdebt']=stock.Data[23]
                 index_data['quick']=stock.Data[24]
                 index_data['wgsd_com_eq_paholder']=stock.Data[25]
                 index_data['longdebttodebt']=stock.Data[26]
                 index_data['tot_liab']=stock.Data[27]
                 index_data['debttoassets']=stock.Data[28]
                 index_data['mkt_cap_ard']=stock.Data[29]
                 index_data['wgsd_com_eq']=stock.Data[30]
                 index_data['close']=stock.Data[31]
                 index_data['trade_status']=stock.Data[32]
                 index_data['data_source']='Wind'
                 index_data['created_date']=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                 #index_data.fillna(0)
                 try:
                    index_data.to_sql(table_name,engine,if_exists='append')
                 except Exception as e:
                     #如果写入数据库失败，写入日志表，便于后续分析处理
                     error_log=pd.DataFrame()
                     error_log['trade_date']=stock.Times
                     error_log['stock_code']=symbol
                     error_log['start_date']=start_date
                     error_log['end_date']=end_date
                     error_log['table']=table_name
                     error_log['args']='Symbol: '+symbol+' From '+start_date+' To '+end_date
                     error_log['error_info']=e
                     error_log['created_date']=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                     error_log.to_sql('stock_error_log',engine,if_exists='append')
                     print   self.getCurrentTime(),": SQL Exception :%s" % (e)
                     continue
                 w.start()
             except Exception as e:
                     #如果读取处理失败，可能是网络中断、频繁访问被限、历史数据缺失等原因。写入相关信息到日志表，便于后续补充处理
                     error_log=pd.DataFrame()
                     error_log['trade_date']=stock.Times
                     error_log['stock_code']=symbol
                     error_log['start_date']=start_date
                     error_log['end_date']=end_date
                     error_log['table']=table_name
                     error_log['args']='Symbol: '+symbol+' From '+start_date+' To '+end_date
                     error_log['error_info']=e
                     error_log['created_date']=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                     error_log.to_sql('stock_error_log',engine,if_exists='append')
                     print  self.getCurrentTime(),":index_data %s : Exception :%s" % (symbol,e)
                     time.sleep(sleep_time)
                     w.start()
                     continue
             print  self.getCurrentTime(),": Downloading [",symbol,"] From "+start_date+" to "+end_date
        print  self.getCurrentTime(),": Download A Stock Has Finished ."

    def getAStockCodesFromCsv(self):
        '''
        获取股票代码清单，链接数据库
        '''
        file_path=os.path.join(os.getcwd(),'Stock.csv')
        stock_code = pd.read_csv(filepath_or_buffer=file_path, encoding='gbk')
        Code=stock_code.code
        return Code

    def getAStockCodesWind(end_date=time.strftime('%Y%m%d',time.localtime(time.time()))):
        '''
        通过wset数据集获取所有A股股票代码，深市代码为股票代码+SZ后缀，沪市代码为股票代码+SH后缀。
        如设定日期参数，则获取参数指定日期所有A股代码，不指定日期参数则默认为当前日期
        :return: 指定日期所有A股代码，不指定日期默认为最新日期
        '''
        w.start()
        #加日期参数取最指定日期股票代码
        #stockCodes=w.wset("sectorconstituent","date="+end_date+";sectorid=a001010100000000;field=wind_code")
        #不加日期参数取最新股票代码
        stockCodes=w.wset("sectorconstituent","sectorid=a001010100000000;field=wind_code")
        return stockCodes.Data[0]
        #return stockCodes

def main(symbols):
    '''
    主调函数，可以通过参数调整实现分批下载
    '''
    global engine,sleep_time,symbols
    sleep_time=5
    windStock=WindStock()
    engine = create_engine('mysql://root:zjz4818774@localhost/investv2?charset=utf8')
    #start_date='20100101'
    #end_date='20131231'
    #symbols=windStock.getAStockCodesFromCsv()#通过文件获取股票代码
    #symbols=windStock.getAStockCodesWind()
    #通过Wind API获取股票代码,默认取最新的，可以指定取历史某一日所有A股代码
    # symbols=['000002.SZ','000008.SZ','000009.SZ','000060.SZ','000063.SZ','000069.SZ','000100.SZ','000156.SZ','000157.SZ']#通过直接赋值获取股票代码用于测试
    # print symbols
    #windStock.AStockHisData(symbols,start_date,end_date)

    # for i in range(2013,1990,-1):
    #      start_date=str(i)+'0101'
    #      end_date=str(i)+'1231'
    #      print start_date,end_date,'Starting'
    #      symbols=windStock.getAStockCodesWind()
    #      windStock.AStockHisData(symbols,start_date,end_date)
    #      print start_date,end_date,'Finished'

    start_date="2008-01-01"
    end_date="2016-12-31"

    print 'Starting'
    windStock.AStockHisData(symbols,start_date,end_date)
    print 'Finished'


if __name__ == "__main__":

    from stocks_pool import select_good_stocks

    symbols = select_good_stocks()
    
    main(symbols)