using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GetPrice
{
    public static class GetOptionsPrice
    {
        public static DataTable GetOptionsPriceFromDB(string ticker="",string tickerHead="",
            Nullable<DateTime> settleDate=null, double strike = double.NaN, string optionType="",MySqlConnection conn=null,string[] columnNames=null)
        {
            string filterString = "";

            if (ticker.Count() > 0)
            { filterString = "WHERE ticker=\"" + ticker + "\""; }
            else if (tickerHead.Count() > 0)
            { filterString = "WHERE ticker_head=\"" + tickerHead + "\""; }

            if (settleDate.HasValue)
            { filterString = filterString + " and price_date=" + ((DateTime)settleDate).ToString("yyyyMMdd"); }

            if (!double.IsNaN(strike))
            {
                filterString = filterString + " and strike=" + strike.ToString();
            }

            if (optionType.Count()>0)
            {
                filterString = filterString + " and option_type=\"" + optionType + "\"";
            }

            columnNames = columnNames ?? new string[]{"id", "option_type", "strike", "cal_dte", "tr_dte", "close_price", "volume", "open_interest"};

            string query = "SELECT " + string.Join(",", columnNames) + " FROM daily_option_price " + filterString;

            MySqlCommand cmd = null;
            MySqlDataReader reader = null;
            DataTable OptionsDataTable = new DataTable();

            try
            {
                cmd = new MySqlCommand(query, conn);
                reader = cmd.ExecuteReader();
                OptionsDataTable.Load(reader);
            }

            finally
            {
                if (reader != null) reader.Close();
            }
            return OptionsDataTable;

        }
    }
}
