1.calculate_parameters文件夹：

    1.1 以begin为前缀的py文件是通过wind的数据接口下载数据存储到本地的mysql数据库中，
        会得到两个数据表:table_day_data，table_month_data。
    
    1.2 以calculate为前缀的py文件是计算我们自己的因子库，并把计算的结果存储到1.1中的table_month_data数据表中。
    
    1.3 recover_month_data.py是用来修复经过1.1和1.2之后的数据表。
    
    1.4 以stocks开头的py文件是用来存储沪深300和中证500的股票信息。
    
    1.5 deal_with_day_data.py是提供一些基本的功能函数。

2.clean文件夹：

    2.1 add_industry_dummy_variables.py是把行业数据添加到table_month_data数据表中

    2.2 get_clean_store_data.py对table_month_data中的数据进行清洗得到一个新的数据表all_stocks_cleaned_factors_table，
        final_clean.py是对all_stocks_cleaned_factors_table中的数据进行清洗得到一个新的数据表final_all_stocks_cleaned_factors_table

    2.3 auto_regression_tvalue_correlation.py是计算得到t值和ic值，
        auto_regression_improved_correlation.py是对ic值计算方法改进后，得到的ic值

3.backtest文件夹：

    3.1 tmp.py进行回测，在控制台会输出每个月末选出的股票组合，该组合中各个股票的收益率，以及该组合总体的收益率，
        最后绘制出2010年到2016年收益率的曲线图。



注意：回测中采用的均是等权重