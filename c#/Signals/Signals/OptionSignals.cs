using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Signals
{
    public static class OptionSignals
    {
        public static DataTable GetOptionTickerIndicators(string ticker = "", string tickerHead = "", Nullable<DateTime> settleDate = null, double delta = 0.5, 
            MySqlConnection conn = null, string[] columnNames = null)
        {

            string filterString = "";

            if (ticker.Count() > 0)
            { filterString = "WHERE ticker=\"" + ticker + "\""; }
            else if (tickerHead.Count() > 0)
            { filterString = "WHERE ticker_head=\"" + tickerHead + "\""; }

            if (settleDate.HasValue)
            { filterString = filterString + " and price_date=" + ((DateTime)settleDate).ToString("yyyyMMdd"); }

            filterString = filterString + " and delta=" + delta.ToString();

            columnNames = columnNames ?? new string[] {"ticker", "price_date" , "ticker_head", "ticker_month",
                "ticker_year", "cal_dte", "tr_dte", "imp_vol", "theta","close2close_vol20", "volume","open_interest"};

            string query = "SELECT " + string.Join(",", columnNames) + " FROM option_ticker_indicators " + filterString;

            MySqlCommand cmd = null;
            MySqlDataReader reader = null;
            DataTable OptionsIndicatorTable = new DataTable();

            try
            {
                cmd = new MySqlCommand(query, conn);
                reader = cmd.ExecuteReader();
                OptionsIndicatorTable.Load(reader);
            }

            finally
            {
                if (reader != null) reader.Close();
            }
            return OptionsIndicatorTable;

        }
    }
}
