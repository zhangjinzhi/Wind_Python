#按季度，前复权，原始货币，交易日
w.wsd("600000.SH", "profit_ttm,bps,fcff,cfps_ttm,wgsd_oper_cf,wgsd_assets,yoy_or,qfa_yoysales,yoyprofit,qfa_yoyprofit,grossprofitmargin,roe_ttm2,roa_ttm2,yoybps,yoy_assets,wgsd_yoyocf,roa,roe,grossprofitmargin_ttm2,assetsturn,faturn,op_ttm2,current,cashtocurrentdebt,quick,wgsd_com_eq_paholder,longdebttodebt,tot_liab,debttoassets", "2016-01-01", "2016-07-01", "unit=1;currencyType=;rptType=1;Period=Q;PriceAdj=F")
#按月，前复权，原始货币，交易日
w.wsd("600000.SH", "mkt_cap_ard,or_ttm,peg,ev2_to_ebitda,mkt_cap_float,eps_ttm,dividendyield,share_ntrd_prfshare,amt,MACD,VMACD,SOBV,RSI", "2016-01-01", "2016-07-01", "unit=1;gRateType=1;currencyType=;rptYear=2016;MACD_L=26;MACD_S=12;MACD_N=9;MACD_IO=1;VMACD_S=12;VMACD_L=26;VMACD_N=9;VMACD_IO=1;RSI_N=6;Period=M;PriceAdj=F")
#原本是按半年，但是没有数据。所以实际我按的是季度
w.wsd("600000.SH", "wgsd_com_eq", "2016-01-01", "2017-01-01", "unit=1;rptType=1;currencyType=;Period=Q;PriceAdj=F")
#按日，前复权，原始货币，交易日
w.wsd("600000.SH,600012.SH", "close", "2016-12-01", "2017-01-01", "PriceAdj=F")