
SELECT dp.id, dp.price_date, sym.ticker, dp.ticker_head, dp.ticker_month, sym.ticker_year, dp.cal_dte, dp.tr_dte, dp.close_price, dp.volume 
FROM symbol as sym INNER JOIN daily_price as dp ON dp.symbol_id = sym.id 
WHERE sym.ticker='EDM2019' ORDER BY dp.price_date, dp.cal_dte


SELECT tr.id, tr.ticker, tr.option_type, tr.strike_price, tr.trade_price, tr.trade_quantity, tr.trade_date, tr.instrument, tr.real_tradeQ
FROM strategy as str INNER JOIN trades as tr ON tr.strategy_id=str.id
WHERE str.alias='BX2016Z2016Z2016F2017_2'