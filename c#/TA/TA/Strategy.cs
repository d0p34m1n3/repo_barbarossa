using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data;
using MySql.Data.MySqlClient;
using DatabaseConnection;
using System.Data;

namespace TA
{
    public static class Strategy
    {
        
        public static DataTable getTrades(string alias, MySqlConnection conn)
        {
            string query = "SELECT tr.id, tr.ticker, tr.option_type, tr.strike_price, tr.trade_price, tr.trade_quantity, tr.trade_date, tr.instrument, tr.real_tradeQ " +
                 "FROM strategy as str INNER JOIN trades as tr ON tr.strategy_id=str.id " +
                 "WHERE str.alias=\"" + alias + "\"";

            MySqlCommand cmd = new MySqlCommand(query, conn);
            MySqlDataReader myReader = cmd.ExecuteReader();
            DataTable myDT = new DataTable();
            myDT.Load(myReader);
            return myDT;
        }

        public static DataTable getNetPosition(string alias, MySqlConnection conn)
        {
            DataTable trades = getTrades(alias,conn);
            trades.Columns.Add("fullTicker", typeof(string));

            foreach (System.Data.DataRow row in trades.Rows)
            {
                if (row["instrument"].ToString() == "F")
                {
                    row["fullTicker"] = row["ticker"];
                }
                else if (row["instrument"].ToString() == "O")
                {
                    row["fullTicker"] = row["ticker"].ToString() + "_" +
                        row["option_type"].ToString() + "_" + row["strike_price"].ToString();
                }
            }

            var grouped = trades.AsEnumerable().GroupBy(r => r["fullTicker"]).Select(w => new
            {
                fullTicker = w.Key.ToString(),
                ticker = w.First()["ticker"].ToString(),
                optionType = w.First()["option_type"].ToString(),
                instrument = w.First()["instrument"].ToString(),
                strikePrice = w.First()["strike_price"].ToString(),
                qty = w.Sum(r => decimal.Parse(r["trade_quantity"].ToString()))
            }).ToList();

            DataTable netPosition = new DataTable();
            netPosition.Columns.Add("ticker", typeof(string));
            netPosition.Columns.Add("optionType", typeof(string));
            netPosition.Columns.Add("strikePrice", typeof(double));
            netPosition.Columns.Add("instrument", typeof(string));
            netPosition.Columns.Add("qty", typeof(double));

            for (int i = 0; i < grouped.Count; i++)
            {
                netPosition.Rows.Add();
                netPosition.Rows[i]["ticker"] = grouped[i].ticker;

                if (grouped[i].strikePrice.Length > 0)
                    netPosition.Rows[i]["strikePrice"] = double.Parse(grouped[i].strikePrice);
                else
                    netPosition.Rows[i]["strikePrice"] = double.NaN;


                netPosition.Rows[i]["optionType"] = grouped[i].optionType;
                netPosition.Rows[i]["instrument"] = grouped[i].instrument;
                netPosition.Rows[i]["qty"] = grouped[i].qty;
            }

            return netPosition;

        }
    }
}
