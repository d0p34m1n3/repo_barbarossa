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
    }
}
