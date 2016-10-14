using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DatabaseConnection
{
    public static class Utils
    {
        public static bool MysqlExecuteManyWrapper(string queryString, List<string> tuples, MySqlConnection conn, int numItemPerLoad=100)
        {

            int i = 0;
            string Sql = null;

            foreach (string Item in tuples)
            {
                i += 1;

                if (i==1)
                {
                    Sql = Item;
                }
                else
                {
                    Sql = Sql + " ," + Item;
                }

                if (i == numItemPerLoad)
                {
                    try
                    {
                        Sql = queryString + Sql;
                        MySqlCommand cmd = new MySqlCommand(Sql, conn);
                        cmd.ExecuteNonQuery();
                        i = 0;
                    }
                    catch
                    {
                        return false;
                    }
                }
            }

            if (i > 0)
            {
                try
                {
                    Sql = queryString + Sql;
                    MySqlCommand cmd = new MySqlCommand(Sql, conn);
                    cmd.ExecuteNonQuery();
                    i = 0;
                }
                catch
                {
                    return false;
                }
            }
            return true;
        }
    }
}
