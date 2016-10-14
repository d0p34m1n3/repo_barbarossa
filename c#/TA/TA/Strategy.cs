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
    public class StrategyInfo
    {
        public int Id { set; get; }
        public DateTime OpenDate { set; get; }
        public DateTime CloseDate { set; get; }
        public double Pnl { set; get; }
        public DateTime CreatedDate { set; get; }
        public DateTime LastUpdatedDate { set; get; }
        public string DescriptionString { set; get; }
    }

    public static class Strategy
    {

        public static StrategyInfo GetStrategyInfoFromAlias(string alias, MySqlConnection conn)
        {
            StrategyInfo StrategyInfoOut = new StrategyInfo();

            string query = "SELECT * FROM strategy WHERE alias=\"" + alias + "\"";

            MySqlDataReader reader = null;
            MySqlCommand cmd = null;

            try
            {
                cmd = new MySqlCommand(query, conn);
                reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    StrategyInfoOut.Id = reader.GetInt32("id");
                    StrategyInfoOut.OpenDate = reader.GetDateTime("open_date");
                    StrategyInfoOut.CloseDate = reader.GetDateTime("close_date");

                    int PnlIndex = reader.GetOrdinal("pnl");

                    StrategyInfoOut.Pnl = reader.IsDBNull(PnlIndex) ? Double.NaN : reader.GetDouble("pnl");

                    StrategyInfoOut.CreatedDate = reader.GetDateTime("created_date");
                    StrategyInfoOut.LastUpdatedDate = reader.GetDateTime("last_updated_date");
                    StrategyInfoOut.DescriptionString = reader.GetString("description_string");
                }
            }

            finally
            {
                if (reader != null) reader.Close();
            }
            return StrategyInfoOut;
        }

        public static bool LoadTrades2Strategy(DataTable tradesTable, string alias, MySqlConnection conn)
        {
            String sqlStart = "insert into trades (ticker, option_type, strike_price, strategy_id, trade_price, trade_quantity, trade_date, instrument, real_tradeQ, created_date, last_updated_date) values ";
            List<string> tuples = new List<string>();

            foreach (DataRow Row in tradesTable.AsEnumerable())
            {
                tuples.Add(String.Format("({0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10})",
                                          "'" + Row.Field<string>("ticker") + "'",
                                         String.IsNullOrEmpty(Row.Field<string>("option_type")) ? "NULL" : "'" + Row.Field<string>("option_type") + "'",
                                         String.IsNullOrEmpty((Row.Field<decimal?>("strike_price").ToString())) ? "NULL" : Row.Field<decimal?>("strike_price").ToString(),
                                         TA.Strategy.GetStrategyInfoFromAlias(alias: Row.Field<string>("alias"), conn: conn).Id,
                                         Row.Field<decimal>("trade_price"),
                                         Row.Field<decimal>("trade_quantity"),
                                         "'" + DateTime.Now.ToString("yyyy-MM-dd") + "'",
                                         "'" + Row.Field<string>("instrument") + "'",
                                         Row.Field<bool>("real_tradeQ"),
                                          "'" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "'",
                                          "'" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "'"
                                         ));
            }

            return DatabaseConnection.Utils.MysqlExecuteManyWrapper(queryString: sqlStart, tuples: tuples, conn: conn);
        }

        public static bool LoadTrade2Strategy(string ticker,decimal trade_price,decimal trade_quantity,string instrument,string alias,MySqlConnection conn,bool real_tradeQ=true,string option_type=null,decimal?strike_price=null)
        {
            String sqlStart = "insert into trades (ticker, option_type, strike_price, strategy_id, trade_price, trade_quantity, trade_date, instrument, real_tradeQ, created_date, last_updated_date) values ";

            String ValuesString = String.Format("({0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10})",
                                          "'" + ticker + "'",
                                         String.IsNullOrEmpty(option_type) ? "NULL" : "'" + option_type + "'",
                                         String.IsNullOrEmpty((strike_price.ToString())) ? "NULL" : strike_price.ToString(),
                                         TA.Strategy.GetStrategyInfoFromAlias(alias: alias, conn: conn).Id,
                                         trade_price,
                                         trade_quantity,
                                         "'" + DateTime.Now.ToString("yyyy-MM-dd") + "'",
                                         "'" + instrument + "'",
                                         real_tradeQ,
                                          "'" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "'",
                                          "'" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "'");

            try
            {
                MySqlCommand cmd = new MySqlCommand(sqlStart + ValuesString, conn);
                cmd.ExecuteNonQuery();
            }
            catch
            {
                return false;
            }
            
            return true;
        }

        
        public static DataTable GetTrades(string alias, MySqlConnection conn)
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

        public static DataTable GetNetPosition(string alias, MySqlConnection conn,DateTime? asOfDate = null)
        {
            DataTable Trades = GetTrades(alias,conn);

            DataTable netPosition = new DataTable();
            netPosition.Columns.Add("Ticker", typeof(string));
            netPosition.Columns.Add("OptionType", typeof(string));
            netPosition.Columns.Add("StrikePrice", typeof(double));
            netPosition.Columns.Add("Instrument", typeof(string));
            netPosition.Columns.Add("Qty", typeof(double));
            DataTable emptyPosition = netPosition.Copy();


            if (asOfDate.HasValue)
            {
                DataRow[] rows = Trades.Select("trade_date<= #" + ((DateTime)asOfDate).ToString("MM/dd/yyyy") + "#");
                if (rows.Count()==0)
                {
                    return emptyPosition;
                }
                Trades = rows.CopyToDataTable();
            }


            Trades.Columns.Add("fullTicker", typeof(string));

            foreach (System.Data.DataRow row in Trades.Rows)
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

            var grouped = Trades.AsEnumerable().GroupBy(r => r["fullTicker"]).Select(w => new
            {
                fullTicker = w.Key.ToString(),
                ticker = w.First()["ticker"].ToString(),
                optionType = w.First()["option_type"].ToString(),
                instrument = w.First()["instrument"].ToString(),
                strikePrice = w.First()["strike_price"].ToString(),
                qty = w.Sum(r => decimal.Parse(r["trade_quantity"].ToString()))
            }).ToList();

            

            for (int i = 0; i < grouped.Count; i++)
            {
                netPosition.Rows.Add();
                netPosition.Rows[i]["Ticker"] = grouped[i].ticker;

                if (grouped[i].strikePrice.Length > 0)
                    netPosition.Rows[i]["StrikePrice"] = double.Parse(grouped[i].strikePrice);
                else
                    netPosition.Rows[i]["StrikePrice"] = double.NaN;


                netPosition.Rows[i]["OptionType"] = grouped[i].optionType;
                netPosition.Rows[i]["Instrument"] = grouped[i].instrument;
                netPosition.Rows[i]["Qty"] = grouped[i].qty;
            }

            DataRow[] NonEmptyPositions = netPosition.Select("qty<>0");

            if (NonEmptyPositions.Count()==0)
            {
                return emptyPosition;
            }

            return NonEmptyPositions.CopyToDataTable();
                
        }

        public static DataTable GetOpenStrategies(DateTime asOfDate,MySqlConnection conn)
        {
            string query = "SELECT * FROM futures_master.strategy WHERE close_date>=" + asOfDate.ToString("yyyyMMdd") + " and open_date<=" + asOfDate.ToString("yyyyMMdd");

            MySqlCommand cmd = null;
            MySqlDataReader reader = null;
            DataTable StrategyTable = new DataTable();

            try
            {
                cmd = new MySqlCommand(query, conn);
                reader = cmd.ExecuteReader();
                StrategyTable.Load(reader);
            }

            finally
            {
                if (reader != null) reader.Close();
            }
            return StrategyTable;
        }

        public static List<string> GetFilteredOpenStrategyList(DateTime asOfDate,MySqlConnection conn,string strategyClass=null, List<string> strategyClassList=null)
        {
            DataTable StrategyTable = GetOpenStrategies(asOfDate: asOfDate, conn: conn);
            StrategyTable.Columns.Add("StrategyClass", typeof(string));

            foreach (DataRow Row in StrategyTable.Rows)
            {
                Row["StrategyClass"] = Shared.Converters.ConvertFromStringToDictionary(stringInput: Row.Field<string>("description_string"))["strategy_class"];
            }

            if (!string.IsNullOrEmpty(strategyClass))
            {
                return (from x in StrategyTable.AsEnumerable()
                        where x.Field<string>("StrategyClass") == strategyClass
                        select x.Field<string>("alias")).ToList();
            }
            else
            {
                return null;
            }

            
        }

        public static string GenerateDbStrategyFromAlias(string alias,string descriptionString,MySqlConnection conn)
        {
            string aliasModified = alias;

            for (int i = 1; i < 11; i++)
			{
			 if (i>1)
             {
                 aliasModified = alias + "_" + i.ToString();
             }

             StrategyInfo Si = Strategy.GetStrategyInfoFromAlias(alias: aliasModified, conn: conn);

                if (Si.OpenDate==DateTime.MinValue)
                    break;
			}

            String sqlStart = "INSERT INTO strategy (alias, open_date, close_date, created_date, last_updated_date, description_string) values ";

            String ValuesString = String.Format("({0},{1},{2},{3},{4},{5})",
                "'" + aliasModified + "'",
                "'" + DateTime.Now.ToString("yyyy-MM-dd") + "'",
                "'" + (new DateTime(3000,1,1)).ToString("yyyy-MM-dd") + "'",
                 "'" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "'",
                  "'" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "'",
                   "'" + descriptionString + "'");

            MySqlCommand cmd = new MySqlCommand(sqlStart+ValuesString, conn);
            cmd.ExecuteNonQuery();
            return aliasModified;

        }

        public static bool CheckIfStrategyExist(string alias,MySqlConnection conn)
        {
            StrategyInfo Si = GetStrategyInfoFromAlias(alias: alias, conn: conn);
            return !(Si.OpenDate == DateTime.MinValue);

        }
    }
}
