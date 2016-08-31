using MySql.Data.MySqlClient;
using MySql.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ContractUtilities
{
    public static class Expiration
    {
        public static Nullable<DateTime> getExpirationFromDB(string ticker,string instrument,MySqlConnection conn)
        {
            string query = "SELECT expiration_date FROM futures_master.symbol WHERE instrument=\"" + instrument + "\" and ticker=\"" + ticker + "\"";
            Nullable<DateTime> expirationDate = null;
            MySqlCommand cmd = null;
            MySqlDataReader reader = null;

            try
            {
                cmd = new MySqlCommand(query, conn);
                reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    expirationDate = reader.GetDateTime("expiration_date");
                }  
            }

            finally
            {
                if (reader != null) reader.Close();
            }
            return expirationDate;
            
        }
    }
}
