using DatabaseConnection;
using MySql.Data.MySqlClient;
using Shared;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TradingTechnologies.TTAPI;
using TradingTechnologies.TTAPI.Autospreader;
using TradingTechnologies.TTAPI.Tradebook;
using ttapiUtils;

namespace IFS_Algo
{
    class Algo:IDisposable
    {
        private string m_username;
        private string m_password;
        public ttapiUtils.Subscription TTAPISubs;
        DataTable MarketPriceTable;
        DataTable LiquidSpreads;
        List<string> ASENameList;
        List<string> TagList;
        public ASInstrumentTradeSubscription Ts = null;
        int NumBets;
        double MaxBetSize;
        List<ttapiUtils.AutoSpreader> AutoSpreaderList;
        mysql connection;
        MySqlConnection conn;
        Logger IFSLogger;

        public Algo(string u, string p)
        {
            m_username = u;
            m_password = p;

            NumBets = 1;
            MaxBetSize = 900;

            connection = new mysql();
            conn = connection.conn;

            string OutputFolder = TA.DirectoryNames.GetDirectoryName("daily");
            StreamWriter LogFile = new StreamWriter(OutputFolder + "/IFS.txt", true);
            IFSLogger = new Logger(LogFile);

            DateTime ReportDate = CalendarUtilities.BusinessDays.GetBusinessDayShifted(shiftInDays: -1);
            DateTime TodayDate = DateTime.Now.Date;

            IFSLogger.SW.WriteLine();
            IFSLogger.Log("NOTES FOR " + TodayDate.ToString("MM/dd/yyyy"));
            IFSLogger.Log(new String('-', 20));

            string DirectoryName = TA.DirectoryNames.GetDirectoryName(ext: "ifsOutput");
            string DirectoryExtension = TA.DirectoryNames.GetDirectoryExtension(directoryDate: ReportDate);

            DataSet PythonOutput = IOUtilities.ExcelDataReader.LoadFile(DirectoryName + "/" + DirectoryExtension + "/ifs.xlsx");
            DataTable IfsSheet = PythonOutput.Tables["all"];

            DataColumn ExistingPositionColumn = new DataColumn("ExistingPosition", typeof(int));
            ExistingPositionColumn.DefaultValue = 0;
            IfsSheet.Columns.Add(ExistingPositionColumn);

            DataColumn ExistingPositionAbsColumn =  new DataColumn("ExistingPositionAbs", typeof(int));
            ExistingPositionAbsColumn.DefaultValue = 0;
            IfsSheet.Columns.Add(ExistingPositionAbsColumn);

            IfsSheet.Columns.Add("Alias", typeof(string));

            AutoSpreaderList = new List<ttapiUtils.AutoSpreader>();

            MarketPriceTable = new DataTable();
            MarketPriceTable.Columns.Add("Ticker", typeof(string));
            MarketPriceTable.Columns.Add("TickerHead", typeof(string));
            MarketPriceTable.Columns.Add("Tag", typeof(string));
            MarketPriceTable.Columns.Add("Alias", typeof(string));
            MarketPriceTable.Columns.Add("DescriptionString", typeof(string));
            MarketPriceTable.Columns.Add("Settle", typeof(decimal));
            MarketPriceTable.Columns.Add("Bid", typeof(decimal));
            MarketPriceTable.Columns.Add("Ask", typeof(decimal));
            MarketPriceTable.Columns.Add("Mid", typeof(decimal));
            MarketPriceTable.Columns.Add("MidConverted", typeof(decimal));
            MarketPriceTable.Columns.Add("SuggestedPosition", typeof(int));
            MarketPriceTable.Columns.Add("WorkingPosition", typeof(int));
            DataColumn ExistingPositionColumn2 = new DataColumn("ExistingPosition", typeof(int));
            ExistingPositionColumn2.DefaultValue = 0;
            MarketPriceTable.Columns.Add(ExistingPositionColumn2);

            DataColumn ExistingPositionTickerHeadColumn = new DataColumn("ExistingPositionTickerHead", typeof(int));
            ExistingPositionTickerHeadColumn.DefaultValue = 0;
            MarketPriceTable.Columns.Add(ExistingPositionTickerHeadColumn);

            MarketPriceTable.Columns.Add("Mean", typeof(double));
            MarketPriceTable.Columns.Add("Std", typeof(double));

            List<string> OpenStrategyList = TA.Strategy.GetFilteredOpenStrategyList(asOfDate: TodayDate, conn: conn, strategyClass: "ifs");

            for (int i = 0; i < OpenStrategyList.Count; i++)
            {
                StrategyUtilities.PositionManagerOutput PositionManagerOut = PositionManager.GetIfsPosition(alias: OpenStrategyList[i], asOfDate: TodayDate, conn: conn);

                if (PositionManagerOut.CorrectPositionQ)
                {
                    string SelectString = "contract1='" + PositionManagerOut.SortedPosition.Rows[0].Field<string>("Ticker") + "' and contract2='" +
                    PositionManagerOut.SortedPosition.Rows[1].Field<string>("Ticker") + "'";

                    if (PositionManagerOut.SortedPosition.Rows.Count==3)
                    {
                        SelectString = SelectString + " and contract3='" + PositionManagerOut.SortedPosition.Rows[2].Field<string>("Ticker") + "'";
                    }

                    DataRow Row = IfsSheet.Select(SelectString)[0];
                    Row["ExistingPosition"] = Math.Round(PositionManagerOut.Scale);
                    Row["ExistingPositionAbs"] = Math.Abs(PositionManagerOut.Scale);
                    Row["Alias"] = OpenStrategyList[i];
                }
                else
                {
                    IFSLogger.Log("Check " + OpenStrategyList[i] + " ! Position may be incorrect.");
                }  
            }

            DataTable newDataTable = IfsSheet.AsEnumerable()
                   .OrderBy(r => r.Field<string>("spread_description"))
                   .ThenByDescending(r => r.Field<double>("min_volume"))
                   .ThenByDescending(r => r.Field<int>("ExistingPositionAbs"))
                   .CopyToDataTable();

            DataRow[] ExistingPositions = IfsSheet.Select("ExistingPositionAbs>0");

            LiquidSpreads = newDataTable.AsEnumerable().GroupBy(r => r["spread_description"]).Select(w => w.First()).CopyToDataTable();

            LiquidSpreads = LiquidSpreads.Select("upside<" + MaxBetSize).CopyToDataTable();

            foreach (DataRow item in ExistingPositions)
            {
                LiquidSpreads.ImportRow(item);
            }

            LiquidSpreads = LiquidSpreads.AsEnumerable().GroupBy(r => r["ticker"]).Select(w => w.First()).CopyToDataTable();

            for (int i = 0; i < LiquidSpreads.Rows.Count; i++)
            {
                List<string> TickerList = new List<string> { LiquidSpreads.Rows[i].Field<string>("Contract1"), LiquidSpreads.Rows[i].Field<string>("Contract2"), LiquidSpreads.Rows[i].Field<string>("Contract3") };
                TickerList.RemoveAll(item => item == null);
                AutoSpreaderList.Add(new ttapiUtils.AutoSpreader(dbTickerList:TickerList,payUpTicks:2));

                DataRow Row = MarketPriceTable.NewRow();
                Row["Ticker"] = AutoSpreaderList[i].AutoSpreaderName;
                Row["TickerHead"] = LiquidSpreads.Rows[i].Field<string>("spread_description");
                Row["Tag"] = "ifs_" + i.ToString();

                if (LiquidSpreads.Rows[i].Field<int>("ExistingPositionAbs")!=0)
                {
                    Row["Alias"] = LiquidSpreads.Rows[i].Field<string>("alias");
                    Row["ExistingPosition"] = LiquidSpreads.Rows[i].Field<int>("ExistingPosition");
                }
                else
                {
                    Row["Alias"] = LiquidSpreads.Rows[i].Field<string>("spread_description") + "_ifs_" + DateTime.Now.ToString("MMM-yyyy");
                }

                Row["DescriptionString"] = "strategy_class=ifs&betsize=" + MaxBetSize.ToString();
                Row["WorkingPosition"] = 0;
                Row["Settle"] = LiquidSpreads.Rows[i].Field<double>("settle");
                Row["Mean"] = LiquidSpreads.Rows[i].Field<double>("mean1");
                Row["Std"] = LiquidSpreads.Rows[i].Field<double>("std1");

                MarketPriceTable.Rows.Add(Row);
                
            }

            List<string> TickerHeadList = DataAnalysis.DataTableFunctions.GetColumnAsList<string>(dataTableInput: MarketPriceTable, columnName: "TickerHead", uniqueQ: true);

            foreach (string item in TickerHeadList)
            {
                DataRow[] RowList = MarketPriceTable.Select("TickerHead='" + item + "'");
                int TotalPosition = 0;

                foreach (DataRow Row in RowList)
                {
                    TotalPosition += Row.Field<int>("ExistingPosition");
                }

                foreach (DataRow Row in RowList)
                {
                    Row["ExistingPositionTickerHead"] = TotalPosition;
                }

            }


            ASENameList = MarketPriceTable.AsEnumerable().Select(x => x.Field<string>("Ticker")).ToList();
            TagList = MarketPriceTable.AsEnumerable().Select(x => x.Field<string>("Tag")).ToList();

            TTAPISubs = new ttapiUtils.Subscription(m_username, m_password);
            TTAPISubs.AutoSpreaderList = AutoSpreaderList;
            TTAPISubs.AsuUpdateList = new List<EventHandler<AuthenticationStatusUpdateEventArgs>> { TTAPISubs.StartASESubscriptions };
            TTAPISubs.priceUpdatedEventHandler = m_ps_FieldsUpdated;
            TTAPISubs.orderFilledEventHandler = m_ts_OrderFilled;

        }

        void m_ps_FieldsUpdated(object sender, FieldsUpdatedEventArgs e)
        {

            if (e.Error == null)
            {
              
                        int RowIndex = ASENameList.IndexOf(e.Fields.Instrument.Name);

                        MarketPriceTable.Rows[RowIndex]["Bid"] = Convert.ToDecimal(e.Fields.GetDirectBidPriceField().FormattedValue);
                        MarketPriceTable.Rows[RowIndex]["Ask"] = Convert.ToDecimal(e.Fields.GetDirectAskPriceField().FormattedValue);

                        MarketPriceTable.Rows[RowIndex]["Mid"] = (MarketPriceTable.Rows[RowIndex].Field<decimal>("Bid") + MarketPriceTable.Rows[RowIndex].Field<decimal>("Ask")) / 2;

                        MarketPriceTable.Rows[RowIndex]["MidConverted"] = TA.PriceConverters.FromTTAutoSpreader2DB(ttPrice: MarketPriceTable.Rows[RowIndex].Field<decimal>("Mid"), tickerHead: MarketPriceTable.Rows[RowIndex].Field<string>("TickerHead"));

                        double MidPriceConverted = (double)MarketPriceTable.Rows[RowIndex].Field<decimal>("MidConverted");
                        double YesterdayMean = MarketPriceTable.Rows[RowIndex].Field<double>("Mean");
                        double YesterdayStd = MarketPriceTable.Rows[RowIndex].Field<double>("Std");

                        ttapiUtils.AutoSpreader As = AutoSpreaderList[RowIndex];


                        if ((MarketPriceTable.Rows[RowIndex].Field<int>("ExistingPosition")<0) && (MidPriceConverted > YesterdayMean + YesterdayStd))
                        {
                            return;
                        }
                        else if (((MarketPriceTable.Rows[RowIndex].Field<int>("ExistingPosition")<0) && (MidPriceConverted < YesterdayMean + YesterdayStd))||
                                ((MarketPriceTable.Rows[RowIndex].Field<int>("ExistingPosition")>0) && (MidPriceConverted > YesterdayMean - YesterdayStd)))

                        {

                            DataRow[] RowList = MarketPriceTable.Select("TickerHead='" + MarketPriceTable.Rows[RowIndex].Field<string>("TickerHead") + "'");

                            foreach (DataRow Row in RowList)
                            {
                                Row["ExistingPositionTickerHead"] = Row.Field<int>("ExistingPositionTickerHead") - MarketPriceTable.Rows[RowIndex].Field<int>("ExistingPosition");
                            }

                            IFSLogger.Log(MarketPriceTable.Rows[RowIndex].Field<string>("Alias") + " has normalized. Closing position...");

                            ttapiUtils.Trade.SendAutospreaderOrder(instrument: e.Fields.Instrument, instrumentDetails: e.Fields.InstrumentDetails, autoSpreader: As, 
                                qty: -MarketPriceTable.Rows[RowIndex].Field<int>("ExistingPosition"),
                               price: MarketPriceTable.Rows[RowIndex].Field<decimal>("Mid"), orderTag: MarketPriceTable.Rows[RowIndex].Field<string>("Tag"), logger: IFSLogger);

                            MarketPriceTable.Rows[RowIndex]["ExistingPosition"] = 0;
                            
                        }

                        else if (MarketPriceTable.Select("WorkingPosition<>0").Count() >= NumBets)
                            return;

                        else if ((MidPriceConverted > YesterdayMean + YesterdayStd) &&
                                 (MarketPriceTable.Rows[RowIndex].Field<int>("WorkingPosition")==0) &&
                                 (MarketPriceTable.Rows[RowIndex].Field<int>("ExistingPositionTickerHead")>=0))

                        {
                            MarketPriceTable.Rows[RowIndex]["SuggestedPosition"] = -1;
                            MarketPriceTable.Rows[RowIndex]["WorkingPosition"] = -1;

                            IFSLogger.Log(MarketPriceTable.Rows[RowIndex].Field<string>("Alias") + " entering a short position...");


                            ttapiUtils.Trade.SendAutospreaderOrder(instrument: e.Fields.Instrument, instrumentDetails: e.Fields.InstrumentDetails, autoSpreader: As, qty: -1,
                                price: MarketPriceTable.Rows[RowIndex].Field<decimal>("Mid"), orderTag: MarketPriceTable.Rows[RowIndex].Field<string>("Tag"), logger: IFSLogger);

                           
                        }
                        else if ((MidPriceConverted<YesterdayMean-YesterdayStd) && 
                            (MarketPriceTable.Rows[RowIndex].Field<int>("WorkingPosition")==0) &&
                            (MarketPriceTable.Rows[RowIndex].Field<int>("ExistingPositionTickerHead")<=0))
                        {
                            MarketPriceTable.Rows[RowIndex]["SuggestedPosition"] = 1;
                            MarketPriceTable.Rows[RowIndex]["WorkingPosition"] = 1;

                            IFSLogger.Log(MarketPriceTable.Rows[RowIndex].Field<string>("Alias") + " entering a long position...");

                            ttapiUtils.Trade.SendAutospreaderOrder(instrument: e.Fields.Instrument, instrumentDetails: e.Fields.InstrumentDetails, autoSpreader: As, qty: 1,
                                price: MarketPriceTable.Rows[RowIndex].Field<decimal>("Mid"), orderTag: MarketPriceTable.Rows[RowIndex].Field<string>("Tag"), logger: IFSLogger);

                        }
                        else
                        {
                            MarketPriceTable.Rows[RowIndex]["SuggestedPosition"] = 0;
                        }

            }
            else
            {
                if (e.Error.IsRecoverableError == false)
                {
                    Console.WriteLine("Unrecoverable price subscription error: {0}", e.Error.Message);
                    Dispose();
                }
            }
        }
    
        void m_ts_OrderFilled(object sender, OrderFilledEventArgs e)
        {

            string Tag = e.Fill.OrderTag;
            int RowIndex = TagList.IndexOf(Tag);
            string Alias = MarketPriceTable.Rows[RowIndex].Field<string>("Alias");

            //ASENameList[RowIndex].

            if (!TA.Strategy.CheckIfStrategyExist(alias:Alias,conn:conn))
            {
                TA.Strategy.GenerateDbStrategyFromAlias(alias: Alias, descriptionString: MarketPriceTable.Rows[RowIndex].Field<string>("DescriptionString"), conn: conn);
            }

            // ProducttKey and SeriesKey are seperately accesible

            if (e.Fill.IsAutospreaderSyntheticFill)  
            {
                return;
            }

            Instrument Inst = AutoSpreaderList[RowIndex].InstrumentDictionary[e.Fill.InstrumentKey];

            string TickerDB = TA.TickerConverters.ConvertFromTTAPIFields2DB(Inst.Product.ToString(), Inst.Name.ToString());
            string TickerHead = TA.TickerheadConverters.ConvertFromTT2DB(Inst.Product.ToString());
            decimal TradeQuantity;

            if (e.Fill.BuySell==BuySell.Buy)
            {
                TradeQuantity = e.Fill.Quantity;
            }
            else
            {
                TradeQuantity = -e.Fill.Quantity;
            }

            TA.Strategy.LoadTrade2Strategy(ticker: TickerDB, trade_price: (decimal)TA.PriceConverters.FromTT2DB(ttPrice: Convert.ToDecimal(e.Fill.MatchPrice.ToString()),
                tickerHead: TickerHead), trade_quantity: TradeQuantity, instrument: "F", alias: Alias, conn: conn);

            IFSLogger.Log(Alias + ": " + TradeQuantity.ToString() + " " + TickerDB + " for " + TA.PriceConverters.FromTT2DB(ttPrice: Convert.ToDecimal(e.Fill.MatchPrice.ToString()),
                tickerHead: TickerHead).ToString());



            //e.Fill.Quantity

            //e.Fill.BuySell

            //e.Fill.MatchPrice

                






            //tickerDB = TA.TickerConverters.ConvertFromTTAPIFields2DB(inst.Product.ToString(), inst.Name.ToString());
            /*
            if (e.Fill.SiteOrderKey == OrderKey)
            {
                // Our parent order has been filled
                Console.WriteLine("Our parent order has been " + (e.Fill.FillType == FillType.Full ? "fully" : "partially") + " filled");
            }
            else if (e.Fill.ParentKey == OrderKey)
            {
                // A child order of our parent order has been filled
                Console.WriteLine("A child order of our parent order has been " + (e.Fill.FillType == FillType.Full ? "fully" : "partially") + " filled");
            }

            Console.WriteLine("Average Buy Price = {0} : Net Position = {1} : P&L = {2}", ts.ProfitLossStatistics.BuyAveragePrice,
                ts.ProfitLossStatistics.NetPosition, ts.ProfitLoss.AsPrimaryCurrency);
             * */
        }
        
        public void Dispose()
        {
            TTAPISubs.Dispose();
        }

    }
}
