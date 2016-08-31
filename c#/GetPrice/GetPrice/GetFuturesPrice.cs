using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using MySql.Data.MySqlClient;

namespace GetPrice
{
    public static class GetFuturesPrice
    {
        public static DataTable getFuturesPrice4Ticker(string ticker="",string tickerHead="",
            Nullable<DateTime> dateTimeFrom=null,Nullable<DateTime> dateTimeTo=null,MySqlConnection conn=null)
        {
            string filterString = "";

            if (ticker.Count()>0)
            { filterString = "WHERE sym.ticker=\"" + ticker + "\""; }
            else if (tickerHead.Count()>0)
            { filterString = "WHERE dp.ticker_head=\"" + tickerHead + "\""; }

            if (dateTimeFrom.HasValue)
            { filterString = filterString + " and dp.price_date>=" + ((DateTime)dateTimeFrom).ToString("yyyyMMdd"); }

            if (dateTimeTo.HasValue)
            { filterString = filterString + " and dp.price_date<=" + ((DateTime)dateTimeTo).ToString("yyyyMMdd"); }

            string query = "SELECT dp.price_date, sym.ticker, dp.ticker_head, dp.ticker_month, " +
                "sym.ticker_year, dp.cal_dte, dp.tr_dte, " +
                "dp.open_price, dp.high_price, dp.low_price, dp.close_price, dp.volume " +
                "FROM symbol as sym " +
                "INNER JOIN daily_price as dp ON dp.symbol_id = sym.id " +
                filterString +
                " ORDER BY dp.price_date, dp.cal_dte";

            MySqlCommand cmd = null;
            MySqlDataReader reader = null;
            DataTable futuresDataTable = new DataTable();
            
            try
            {
                cmd = new MySqlCommand(query, conn);
                reader = cmd.ExecuteReader();
                futuresDataTable.Load(reader);  
            }

            finally
            {
                if (reader != null) reader.Close();
            }
            return futuresDataTable;

        }
    }
}
