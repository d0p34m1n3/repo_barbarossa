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
using System.Timers;
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
        DataTable SummaryTable;
        DataTable LiquidSpreads;
        List<string> ASENameList;
        List<string> TagList;
        public ASInstrumentTradeSubscription Ts = null;
        int NumBets;
        double MaxBetSize;
        List<ttapiUtils.AutoSpreader> AutoSpreaderList;
        Dictionary<string, ttapiUtils.AutoSpreader> AutoSpreaderDictionary;
        mysql connection;
        MySqlConnection conn;
        Logger IFSLogger;
        System.Timers.Timer TradeTimer;

        public Algo(string u, string p)
        {
            m_username = u;
            m_password = p;

            NumBets = 3;
            MaxBetSize = 700;

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

            TradeTimer = new System.Timers.Timer();
            TradeTimer.Elapsed += new ElapsedEventHandler(PeriodicCall);
            TradeTimer.Interval = 30000;
            // And start it        
            TradeTimer.Enabled = true;

            DataColumn ExistingPositionColumn = new DataColumn("ExistingPosition", typeof(int));
            ExistingPositionColumn.DefaultValue = 0;
            IfsSheet.Columns.Add(ExistingPositionColumn);

            DataColumn ExistingPositionAbsColumn =  new DataColumn("ExistingPositionAbs", typeof(int));
            ExistingPositionAbsColumn.DefaultValue = 0;
            IfsSheet.Columns.Add(ExistingPositionAbsColumn);

            IfsSheet.Columns.Add("Alias", typeof(string));

            AutoSpreaderList = new List<ttapiUtils.AutoSpreader>();
            AutoSpreaderDictionary = new Dictionary<string, ttapiUtils.AutoSpreader>();

            SummaryTable = new DataTable();
            SummaryTable.Columns.Add("Ticker", typeof(string));
            SummaryTable.Columns.Add("TickerHead", typeof(string));
            SummaryTable.Columns.Add("Tag", typeof(string));
            SummaryTable.Columns.Add("Alias", typeof(string));
            SummaryTable.Columns.Add("DescriptionString", typeof(string));
            SummaryTable.Columns.Add("Settle", typeof(decimal));
            SummaryTable.Columns.Add("Bid", typeof(double));
            SummaryTable.Columns.Add("Ask", typeof(double));
            SummaryTable.Columns.Add("Mid", typeof(double));
            SummaryTable.Columns.Add("MidConverted", typeof(double));
            SummaryTable.Columns.Add("SuggestedSize", typeof(int));
            SummaryTable.Columns.Add("WorkingPosition", typeof(int));
            DataColumn ExistingPositionColumn2 = new DataColumn("ExistingPosition", typeof(int));
            ExistingPositionColumn2.DefaultValue = 0;
            SummaryTable.Columns.Add(ExistingPositionColumn2);

            DataColumn ExistingPositionTickerHeadColumn = new DataColumn("ExistingPositionTickerHead", typeof(int));
            ExistingPositionTickerHeadColumn.DefaultValue = 0;
            SummaryTable.Columns.Add(ExistingPositionTickerHeadColumn);

            DataColumn ValidPriceColumn = new DataColumn("ValidPrice", typeof(bool));
            ValidPriceColumn.DefaultValue = false;
            SummaryTable.Columns.Add(ValidPriceColumn);

            SummaryTable.Columns.Add("Mean", typeof(double));
            SummaryTable.Columns.Add("Std", typeof(double));

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
            LiquidSpreads = LiquidSpreads.Select("spread_description='C_W' or spread_description='W_KW' or spread_description='S_BO_SM'").CopyToDataTable();

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

                AutoSpreaderDictionary.Add(AutoSpreaderList[i].AutoSpreaderName, AutoSpreaderList[i]);

                DataRow Row = SummaryTable.NewRow();
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
                Row["Mean"] = LiquidSpreads.Rows[i].Field<double>("mean10");
                Row["Std"] = LiquidSpreads.Rows[i].Field<double>("std10");
                Row["SuggestedSize"] = Math.Min(10,Math.Round(2 * MaxBetSize /(LiquidSpreads.Rows[i].Field<double>("upside") + Math.Abs(LiquidSpreads.Rows[i].Field<double>("downside")))));

                SummaryTable.Rows.Add(Row);
                
            }

            List<string> TickerHeadList = DataAnalysis.DataTableFunctions.GetColumnAsList<string>(dataTableInput: SummaryTable, columnName: "TickerHead", uniqueQ: true);

            foreach (string item in TickerHeadList)
            {
                DataRow[] RowList = SummaryTable.Select("TickerHead='" + item + "'");
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


            ASENameList = SummaryTable.AsEnumerable().Select(x => x.Field<string>("Ticker")).ToList();
            TagList = SummaryTable.AsEnumerable().Select(x => x.Field<string>("Tag")).ToList();

            TTAPISubs = new ttapiUtils.Subscription(m_username, m_password);
            TTAPISubs.AutoSpreaderList = AutoSpreaderList;
            TTAPISubs.AsuUpdateList = new List<EventHandler<AuthenticationStatusUpdateEventArgs>> { TTAPISubs.StartASESubscriptions };
            TTAPISubs.priceUpdatedEventHandler = m_ps_FieldsUpdated;
            TTAPISubs.orderFilledEventHandler = m_ts_OrderFilled;
            TTAPISubs.OrderDeletedEventHandler = OrderDeleted;

        }

        private void PeriodicCall(object source, ElapsedEventArgs e)
        {
            Console.WriteLine(new String('-', 20));

            for (int i = 0; i < SummaryTable.Rows.Count; i++)
            {
                DataRow SelectedRow = SummaryTable.Rows[i];

                if (SelectedRow.Field<bool>("ValidPrice"))
                {
                    double Zscore = (SelectedRow.Field<double>("MidConverted") - SelectedRow.Field<double>("Mean")) / (1.75 * SelectedRow.Field<double>("Std"));

                    ttapiUtils.AutoSpreader SelectedAutospreader = AutoSpreaderList[i];
                    Instrument SelectedInstrument = SelectedAutospreader.AutoSpreaderInstrument;

                    Console.WriteLine(SelectedRow.Field<string>("Ticker") + " Z-Score: " + Math.Round(Zscore,2));

                    if ((SelectedRow.Field<int>("ExistingPosition") < 0) && (Zscore > 1))
                    {
                        continue;
                    }
                    else if (((SelectedRow.Field<int>("ExistingPosition") < 0) && (Zscore < 0.75)) ||
                            ((SelectedRow.Field<int>("ExistingPosition") > 0) && (Zscore > -0.75)))
                    {

                        DataRow[] RowList = SummaryTable.Select("TickerHead='" + SelectedRow.Field<string>("TickerHead") + "'");

                        foreach (DataRow Row in RowList)
                        {
                            Row["ExistingPositionTickerHead"] = Row.Field<int>("ExistingPositionTickerHead") - SelectedRow.Field<int>("ExistingPosition");
                        }

                        IFSLogger.Log(SelectedRow.Field<string>("Alias") + " has normalized. Closing position...");

                        ttapiUtils.Trade.SendAutospreaderOrder(autoSpreader: SelectedAutospreader,
                            qty: -SelectedRow.Field<int>("ExistingPosition"),
                            price: (decimal)SelectedRow.Field<double>("Mid"), orderTag: SelectedRow.Field<string>("Tag"), logger: IFSLogger,reloadQuantity:1);

                        SelectedRow["ExistingPosition"] = 0;

                    }

                    else if (SummaryTable.Select("WorkingPosition<>0").Count() >= NumBets)
                        continue;

                    else if ((Zscore > 1) &&
                             (SelectedRow.Field<int>("WorkingPosition") == 0) &&
                             (SelectedRow.Field<int>("ExistingPositionTickerHead") >= 0))
                    {
                        SelectedRow["WorkingPosition"] = -SelectedRow.Field<int>("SuggestedSize");

                        IFSLogger.Log(SelectedRow.Field<string>("Alias") + " entering a short position...");


                        ttapiUtils.Trade.SendAutospreaderOrder(autoSpreader: SelectedAutospreader,
                            qty: -SelectedRow.Field<int>("SuggestedSize"), price: (decimal)SelectedRow.Field<double>("Mid"), orderTag: SelectedRow.Field<string>("Tag"), logger: IFSLogger, reloadQuantity:1);


                    }
                    else if ((Zscore < -1) &&
                        (SelectedRow.Field<int>("WorkingPosition") == 0) &&
                        (SelectedRow.Field<int>("ExistingPositionTickerHead") <= 0))
                    {
                        SelectedRow["WorkingPosition"] = SelectedRow.Field<int>("SuggestedSize");

                        IFSLogger.Log(SelectedRow.Field<string>("Alias") + " entering a long position...");

                        ttapiUtils.Trade.SendAutospreaderOrder(autoSpreader: SelectedAutospreader, 
                            qty: SelectedRow.Field<int>("SuggestedSize"),
                            price: (decimal)SelectedRow.Field<double>("Mid"), orderTag: SelectedRow.Field<string>("Tag"), logger: IFSLogger, reloadQuantity:1);

                    }
                }

            }


        }

        void m_ps_FieldsUpdated(object sender, FieldsUpdatedEventArgs e)
        {

            if (e.Error == null)
            {
              
                        int RowIndex = ASENameList.IndexOf(e.Fields.Instrument.Name);

                        Price BidPrice = e.Fields.GetDirectBidPriceField().Value;
                        Price AskPrice = e.Fields.GetDirectAskPriceField().Value;

                        double BidPriceDb = BidPrice.ToDouble();
                        double AskPriceDb = AskPrice.ToDouble();

                        SummaryTable.Rows[RowIndex]["Bid"] = BidPrice.ToDouble();
                        SummaryTable.Rows[RowIndex]["Ask"] = AskPrice.ToDouble();

                        SummaryTable.Rows[RowIndex]["Mid"] = (SummaryTable.Rows[RowIndex].Field<double>("Bid") + SummaryTable.Rows[RowIndex].Field<double>("Ask")) / 2;

                        SummaryTable.Rows[RowIndex]["MidConverted"] = TA.PriceConverters.FromTTAutoSpreader2DB(ttPrice: (decimal)SummaryTable.Rows[RowIndex].Field<double>("Mid"), tickerHead: SummaryTable.Rows[RowIndex].Field<string>("TickerHead"));

                        SummaryTable.Rows[RowIndex]["ValidPrice"] = true; 

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
            string Alias = SummaryTable.Rows[RowIndex].Field<string>("Alias");

            //ASENameList[RowIndex].

            if (!TA.Strategy.CheckIfStrategyExist(alias:Alias,conn:conn))
            {
                TA.Strategy.GenerateDbStrategyFromAlias(alias: Alias, descriptionString: SummaryTable.Rows[RowIndex].Field<string>("DescriptionString"), conn: conn);
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

        }

        void OrderDeleted(object sender, OrderDeletedEventArgs e)
        {

           
        }
        
        public void Dispose()
        {
            TTAPISubs.Dispose();
        }

    }
}
