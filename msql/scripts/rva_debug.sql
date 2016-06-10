

SELECT * FROM futures_master.strategy WHERE close_date>20160605
SELECT * FROM futures_master.strategy

SELECT dp.id, dp.price_date, sym.ticker, dp.ticker_head, dp.ticker_month, sym.ticker_year, dp.cal_dte, dp.tr_dte, dp.close_price, dp.volume 
FROM symbol as sym INNER JOIN daily_price as dp ON dp.symbol_id = sym.id 
WHERE sym.ticker='EDZ2018' ORDER BY dp.price_date, dp.cal_dte



SELECT dp.id, dp.price_date, sym.ticker, dp.ticker_head, dp.ticker_month, sym.ticker_year, dp.cal_dte, dp.tr_dte, dp.close_price, dp.volume 
FROM symbol as sym INNER JOIN daily_price as dp ON dp.symbol_id = sym.id 
WHERE sym.ticker='NGF2008' AND dp.price_date>=20070222 AND dp.price_date<=20070228 ORDER BY dp.price_date, dp.cal_dte


SELECT dp.id, dp.price_date, sym.ticker, dp.ticker_head, dp.ticker_month, sym.ticker_year, dp.cal_dte, dp.tr_dte, dp.close_price, dp.volume 
FROM symbol as sym INNER JOIN daily_price as dp ON dp.symbol_id = sym.id 
WHERE sym.ticker='HOX2009' AND dp.price_date>=20090310 AND dp.price_date<=20090320 ORDER BY dp.price_date, dp.cal_dte


SELECT dp.id, dp.price_date, sym.ticker, dp.ticker_head, dp.ticker_month, sym.ticker_year, dp.cal_dte, dp.tr_dte, dp.close_price, dp.volume 
FROM symbol as sym INNER JOIN daily_price as dp ON dp.symbol_id = sym.id 
WHERE sym.ticker='HOZ2009' AND dp.price_date>=20090310 AND dp.price_date<=20090320 ORDER BY dp.price_date, dp.cal_dte


DELETE from strategy where id=102



SELECT tr.id, tr.ticker, tr.option_type, tr.strike_price, tr.trade_price, tr.trade_quantity, tr.trade_date, tr.instrument, tr.real_tradeQ
FROM strategy as str INNER JOIN trades as tr ON tr.strategy_id=str.id
WHERE str.alias='delta_may'


INSERT INTO strategy 



