using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using MySql.Data;
using MySql.Data.MySqlClient;

namespace DatabaseConnection
{
    public class mysql
    {
        public MySqlConnection conn
        {
            get;
            set;
        }

        public mysql()
        {
            string connStr = "server=localhost;user=ekocatulum;database=futures_master;port=3306;password=pompei1789;";
            conn = new MySqlConnection(connStr);
            conn.Open();
        }
    }
}
