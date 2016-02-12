

SELECT * FROM futures_master.strategy WHERE close_date<20160212

SELECT dp.id, dp.price_date, sym.ticker, dp.ticker_head, dp.ticker_month, sym.ticker_year, dp.cal_dte, dp.tr_dte, dp.close_price, dp.volume 
FROM symbol as sym INNER JOIN daily_price as dp ON dp.symbol_id = sym.id 
WHERE sym.ticker='EDZ2018' ORDER BY dp.price_date, dp.cal_dte



SELECT dp.id, dp.price_date, sym.ticker, dp.ticker_head, dp.ticker_month, sym.ticker_year, dp.cal_dte, dp.tr_dte, dp.close_price, dp.volume 
FROM symbol as sym INNER JOIN daily_price as dp ON dp.symbol_id = sym.id 
WHERE sym.ticker='BN2017' AND dp.price_date>=20160127 AND dp.price_date<=20160210 ORDER BY dp.price_date, dp.cal_dte


SELECT dp.id, dp.price_date, sym.ticker, dp.ticker_head, dp.ticker_month, sym.ticker_year, dp.cal_dte, dp.tr_dte, dp.close_price, dp.volume 
FROM symbol as sym INNER JOIN daily_price as dp ON dp.symbol_id = sym.id 
WHERE sym.ticker='HOF2010' AND dp.price_date>=20090317 AND dp.price_date<=20090319 ORDER BY dp.price_date, dp.cal_dte


SELECT dp.id, dp.price_date, sym.ticker, dp.ticker_head, dp.ticker_month, sym.ticker_year, dp.cal_dte, dp.tr_dte, dp.close_price, dp.volume 
FROM symbol as sym INNER JOIN daily_price as dp ON dp.symbol_id = sym.id 
WHERE sym.ticker='HOG2010' AND dp.price_date>=20090317 AND dp.price_date<=20090319 ORDER BY dp.price_date, dp.cal_dte


#DELETE from daily_price where id=2476414



SELECT tr.id, tr.ticker, tr.option_type, tr.strike_price, tr.trade_price, tr.trade_quantity, tr.trade_date, tr.instrument, tr.real_tradeQ
FROM strategy as str INNER JOIN trades as tr ON tr.strategy_id=str.id
WHERE str.alias='B_pca_Jan16'


INSERT INTO strategy 



