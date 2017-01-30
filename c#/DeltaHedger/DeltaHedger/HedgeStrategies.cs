using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DeltaHedger
{
    public static class HedgeStrategies
    {
        
        public static DataTable GetHedge4Strategy(string alias,MySqlConnection conn,DataTable priceTable)
        {
            DateTime CurrentDate = DateTime.Now.Date;
            DateTime SettlePriceDate = CalendarUtilities.BusinessDays.GetBusinessDayShifted(referanceDate: CurrentDate, shiftInDays: -1);

            DataAnalysis.DataTableFunctions.CleanNullRows(priceTable, new string[] { "MidPrice" });

            DataTable HedgeTable = new DataTable();
            HedgeTable.Columns.Add("Ticker", typeof(string));
            HedgeTable.Columns.Add("Hedge", typeof(decimal));
            HedgeTable.Columns.Add("HedgePrice", typeof(decimal));
            HedgeTable.Columns.Add("OptionDelta", typeof(decimal));
            HedgeTable.Columns.Add("UnderlyingDelta", typeof(decimal));
            HedgeTable.Columns.Add("TotalDelta", typeof(decimal));

            DataTable Position = TA.Strategy.GetNetPosition(alias: alias, conn: conn, asOfDate: CurrentDate);

            if (Position.Rows.Count == 0)
            {
                return HedgeTable;
            }

            DataRow[] OptionsRows = Position.Select("instrument='O'");
            DataRow[] FuturesRows = Position.Select("instrument='F'");

            DataTable FuturesTable = new DataTable();

            if (FuturesRows.Count() > 0)
            {
                FuturesTable = FuturesRows.CopyToDataTable();
                FuturesTable.Columns["qty"].ColumnName = "UnderlyingDelta";
            }

            // If there are no options in the strategy, all the underlying
            // position should be transferred
            if (OptionsRows.Count() == 0)
            {

                // make sure the price for all the instruments exist
                for (int i = 0; i < FuturesTable.Rows.Count; i++)
                {
                    bool PriceFoundQ = priceTable.AsEnumerable().Any(row => FuturesTable.Rows[i].Field<string>("Ticker") == row.Field<string>("Ticker"));
                    if (!PriceFoundQ)
                    {
                        return HedgeTable;
                    }
                }
                var results = from table1 in FuturesTable.AsEnumerable()
                              join table2 in priceTable.AsEnumerable() on
                              table1["Ticker"] equals table2["Ticker"]
                              select new
                              {
                                  Ticker = table1["Ticker"],
                                  UnderlyingDelta = Convert.ToDecimal(table1["UnderlyingDelta"]),
                                  HedgePrice = Convert.ToDecimal(table2["MidPrice"])
                              };

                foreach (var result in results)
                {
                    DataRow row = HedgeTable.NewRow();
                    row["Ticker"] = result.Ticker;
                    row["Hedge"] = -result.UnderlyingDelta;
                    row["UnderlyingDelta"] = result.UnderlyingDelta;
                    row["TotalDelta"] = result.UnderlyingDelta;
                    row["HedgePrice"] = result.HedgePrice;
                    HedgeTable.Rows.Add(row);

                }

                return HedgeTable;
            }

            DataTable OptionsTable = OptionsRows.CopyToDataTable();
            OptionsTable.Columns.Add("ImpVol", typeof(double));
            OptionsTable.Columns.Add("UnderlyingTicker", typeof(string));
            OptionsTable.Columns.Add("UnderlyingPrice", typeof(decimal));
            OptionsTable.Columns.Add("Delta", typeof(double));
            OptionsTable.Columns.Add("TotalDelta", typeof(double));

            foreach (DataRow Row in OptionsTable.Rows)
            {
                DataTable ImpVolTable = GetPrice.GetOptionsPrice.GetOptionsPriceFromDB(ticker: Row.Field<string>("Ticker"),
                    settleDate: SettlePriceDate, strike: Row.Field<double>("StrikePrice"),
                    conn: conn, columnNames: new string[] { "imp_vol" });
                DataAnalysis.DataTableFunctions.CleanNullRows(ImpVolTable, new string[] { "imp_vol" });

                if (ImpVolTable.Rows.Count==0)
                {
                    return HedgeTable;
                }

                Row["ImpVol"] = ImpVolTable.Rows[0].Field<decimal>("imp_vol");
                Row["UnderlyingTicker"] = OptionModels.Utils.GetOptionUnderlying(Row.Field<string>("Ticker"));

                DataRow[] PriceRows = priceTable.Select("Ticker='" + Row["UnderlyingTicker"] + "'");

                if (PriceRows.Count() == 0)
                {
                    return HedgeTable;
                }

                Row["UnderlyingPrice"] = PriceRows[0].Field<decimal>("MidPrice");

                Row["Delta"] = OptionModels.Utils.OptionModelWrapper(modelName: "BS", ticker: Row.Field<string>("Ticker"),
                    optionType: Row.Field<string>("OptionType"), strike: Row.Field<double>("StrikePrice"), conn: conn,
                    calculationDate: CurrentDate, impliedVol: Row.Field<double>("ImpVol"),
                    underlyingPrice: (double)Row.Field<decimal>("UnderlyingPrice"), interestRateDate: SettlePriceDate).Delta;

                if (double.IsNaN(Row.Field<double>("Delta")))
                {
                    return HedgeTable;
                }

                Row["TotalDelta"] = Row.Field<double>("Qty") * Row.Field<double>("Delta");

            }

            var grouped = OptionsTable.AsEnumerable().GroupBy(r => r["UnderlyingTicker"]).Select(w => new
            {
                Ticker = w.Key.ToString(),
                HedgePrice = w.First()["UnderlyingPrice"],
                OptionDelta = Math.Round(w.Sum(r => decimal.Parse(r["TotalDelta"].ToString())), 2)
            }).ToList();

            for (int i = 0; i < grouped.Count; i++)
            {
                HedgeTable.Rows.Add();
                HedgeTable.Rows[i]["Ticker"] = grouped[i].Ticker;
                HedgeTable.Rows[i]["HedgePrice"] = grouped[i].HedgePrice;
                HedgeTable.Rows[i]["OptionDelta"] = grouped[i].OptionDelta;
                HedgeTable.Rows[i]["UnderlyingDelta"] = 0;
                HedgeTable.Rows[i]["TotalDelta"] = grouped[i].OptionDelta;
                HedgeTable.Rows[i]["Hedge"] = -grouped[i].OptionDelta;
            }

            if (FuturesRows.Count() > 0)
            {
                List<string> TickerList = HedgeTable.AsEnumerable().Select(r => r.Field<string>("Ticker")).ToList();

                foreach (DataRow FuturesRow in FuturesTable.Rows)
                {
                    int TickerIndex = TickerList.IndexOf(FuturesRow.Field<string>("Ticker"));

                    if (TickerIndex >= 0)
                    {
                        HedgeTable.Rows[TickerIndex]["UnderlyingDelta"] = FuturesRow.Field<double>("UnderlyingDelta");
                        HedgeTable.Rows[TickerIndex]["TotalDelta"] = HedgeTable.Rows[TickerIndex].Field<decimal>("OptionDelta") +
                            HedgeTable.Rows[TickerIndex].Field<decimal>("UnderlyingDelta");
                        HedgeTable.Rows[TickerIndex]["Hedge"] = -HedgeTable.Rows[TickerIndex].Field<decimal>("TotalDelta");
                    }
                    else
                    {
                        DataRow HedgeRow = HedgeTable.NewRow();
                        HedgeRow["Ticker"] = FuturesRow.Field<string>("Ticker");
                        DataRow[] PriceRows = priceTable.Select("Ticker='" + FuturesRow.Field<string>("Ticker") + "'");

                        if (PriceRows.Count() == 0)
                        {
                            return HedgeTable;
                        }

                        HedgeRow["HedgePrice"] = PriceRows[0].Field<decimal>("MidPrice");
                        HedgeRow["OptionDelta"] = 0;
                        HedgeRow["UnderlyingDelta"] = FuturesRow.Field<double>("UnderlyingDelta");
                        HedgeRow["TotalDelta"] = FuturesRow.Field<double>("UnderlyingDelta");
                        HedgeRow["Hedge"] = -FuturesRow.Field<double>("UnderlyingDelta");
                        HedgeTable.Rows.Add(HedgeRow);
                    }
                }

            }
            return HedgeTable;
        }

        public static List<string> HedgeStrategyAgainstDelta(string alias, string deltaAlias, MySqlConnection conn,DataTable priceTable)
        {
            DataTable HedgeTable = GetHedge4Strategy(alias: alias, conn: conn,priceTable:priceTable);

            List<string> HedgeNotes = new List<string>();

            DataRow[] HedgeRows = HedgeTable.Select("Hedge<>0");

            if (HedgeRows.Count() == 0)
            {
                HedgeNotes.Add(alias + ": No hedge necessary");
                return HedgeNotes;
            }

            DataTable Trades2Enter = new DataTable();
            Trades2Enter.Columns.Add("ticker", typeof(string));
            Trades2Enter.Columns.Add("option_type", typeof(string));
            Trades2Enter.Columns.Add("strike_price", typeof(decimal));
            Trades2Enter.Columns.Add("trade_price", typeof(decimal));
            Trades2Enter.Columns.Add("trade_quantity", typeof(decimal));
            Trades2Enter.Columns.Add("instrument", typeof(string));
            Trades2Enter.Columns.Add("real_tradeQ", typeof(bool));
            Trades2Enter.Columns.Add("alias", typeof(string));

            foreach (DataRow HedgeRow in HedgeRows)
            {
                DataRow TradesRow = Trades2Enter.NewRow();
                TradesRow["ticker"] = HedgeRow.Field<string>("Ticker");
                TradesRow["trade_price"] = HedgeRow.Field<decimal>("HedgePrice");
                TradesRow["trade_quantity"] = HedgeRow.Field<decimal>("Hedge");
                TradesRow["instrument"] = "F";
                TradesRow["real_tradeQ"] = true;
                TradesRow["alias"] = alias;
                Trades2Enter.Rows.Add(TradesRow);
            }

            bool loadResult = TA.Strategy.LoadTrades2Strategy(tradesTable: Trades2Enter, alias: alias, conn: conn);

            if (!loadResult)
            {
                HedgeNotes.Add(alias + ": Hedge trades cannot be loaded to the strategy");
                return HedgeNotes;
            }

            Trades2Enter.Columns["alias"].Expression = "'"+deltaAlias+"'";

            foreach (DataRow Row in Trades2Enter.AsEnumerable())
            {
                Row["trade_quantity"] = -1 * Row.Field<decimal>("trade_quantity");
            }

            loadResult = TA.Strategy.LoadTrades2Strategy(tradesTable: Trades2Enter, alias: alias, conn: conn);

            if (!loadResult)
            {
                HedgeNotes.Add(alias + ": Hedge trades cannot be loaded to the delta");
                return HedgeNotes;
            }
            return HedgeNotes;
        }

        public static List<string> GetStrategyList2Hedge(MySqlConnection conn)
        {
            DateTime AsOfDate = DateTime.Now.Date;
            DataTable StrategyTable = TA.Strategy.GetOpenStrategies(asOfDate: AsOfDate, conn: conn);
            StrategyTable.Columns.Add("StrategyClass", typeof(string));

            foreach (DataRow Row in StrategyTable.Rows)
            {
                Row["StrategyClass"] = Shared.Converters.ConvertFromStringToDictionary(stringInput: Row.Field<string>("description_string"))["strategy_class"];
            }

            List<string> OptionStrategyClassList = new List<string> { "vcs", "scv", "optionInventory" };

            List<string> StList = (from x in StrategyTable.AsEnumerable()
                                   where OptionStrategyClassList.Any(b => x.Field<string>("StrategyClass").Contains(b))
                                   select x.Field<string>("alias")).ToList();

            //StList.Remove("SI_inventory_Aug16");
            return StList;
        }

        public static List<string> GetDeltaStrategyList(MySqlConnection conn)
        {
            DateTime AsOfDate = DateTime.Now.Date;
            DataTable StrategyTable = TA.Strategy.GetOpenStrategies(asOfDate: AsOfDate, conn: conn);
            StrategyTable.Columns.Add("StrategyClass", typeof(string));

            foreach (DataRow Row in StrategyTable.Rows)
            {
                Row["StrategyClass"] = Shared.Converters.ConvertFromStringToDictionary(stringInput: Row.Field<string>("description_string"))["strategy_class"];
            }

            return (from x in StrategyTable.AsEnumerable()
                    where x.Field<string>("StrategyClass")=="delta"
                    select x.Field<string>("alias")).ToList();
        }

        public static string GetActiveDeltaStrategy(MySqlConnection conn)
        {
            return HedgeStrategies.GetDeltaStrategyList(conn: conn).Last();
        }

        public static void HedgeStrategiesAgainstDelta(MySqlConnection conn,DataTable priceTable)
        {
            List<string> HedgeStrategyList = HedgeStrategies.GetStrategyList2Hedge(conn: conn);
            List<string> DeltaStrategyList = HedgeStrategies.GetDeltaStrategyList(conn: conn);
            string ActiveDeltaStrategy = DeltaStrategyList[DeltaStrategyList.Count - 1];

            Console.WriteLine("Calculating hedges for strategies...");

            foreach (string item in HedgeStrategyList)
            {
                Console.WriteLine(item);
                HedgeStrategies.HedgeStrategyAgainstDelta(alias: item, deltaAlias: ActiveDeltaStrategy, conn: conn,priceTable:priceTable);
            }
        }

    }
}
