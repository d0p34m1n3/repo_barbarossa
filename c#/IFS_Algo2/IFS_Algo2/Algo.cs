using Shared;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Timers;
using System.Threading.Tasks;
using System.Data;
using TradingTechnologies.TTAPI;
using TradingTechnologies.TTAPI.Autospreader;
using TradingTechnologies.TTAPI.Tradebook;
using MySql.Data.MySqlClient;
using DatabaseConnection;


namespace IFS_Algo2
{
    class Algo:IDisposable
    {
        private string m_username;
        private string m_password;
        public ttapiUtils.Subscription TTAPISubs;
        Logger IFSLogger;
        List<ttapiUtils.AutoSpreader> AutoSpreaderList;
        Dictionary<string, ttapiUtils.AutoSpreader> AutoSpreaderDictionary;
        DataTable LiquidSpreads;
        System.Timers.Timer MyTimer;
        DataTable SummaryTable;
        List<string> ASENameList;
        List<string> TagList;
        DateTime TodayDate;
        DataAnalysis.TimeSeries TimeSeriesObj;
        MySqlConnection conn;
        enum OpportunityZone {Zone0,Zone1, Zone2, Zone3, Zone4};
        enum PositionZone { None, Long, Short, WorkingEntryLong, WorkingExitLong, WorkingEntryShort, WorkingExitShort, Undefined};
        

        public Algo(string u, string p)
        {
            m_username = u;
            m_password = p;

            mysql connection = new mysql();
            conn = connection.conn;

            string OutputFolder = TA.DirectoryNames.GetDirectoryName("daily");
            StreamWriter LogFile = new StreamWriter(OutputFolder + "/IFS2.txt", true);
            IFSLogger = new Logger(LogFile);

            DateTime ReportDate = CalendarUtilities.BusinessDays.GetBusinessDayShifted(shiftInDays: -1);
            TodayDate = DateTime.Now.Date;

            DateTime startDate = DateTime.Now;
            
            DateTime StartTime = new DateTime(TodayDate.Year, TodayDate.Month, TodayDate.Day, 8, 30, 0);
            DateTime EndTime = new DateTime(TodayDate.Year, TodayDate.Month, TodayDate.Day, 15, 0, 0);
            
            int minInterval = 1;

            IFSLogger.SW.WriteLine();
            IFSLogger.Log("NOTES FOR " + TodayDate.ToString("MM/dd/yyyy"));
            IFSLogger.Log(new String('-', 20));

            string DirectoryName = TA.DirectoryNames.GetDirectoryName(ext: "ifsOutput");
            string DirectoryExtension = TA.DirectoryNames.GetDirectoryExtension(directoryDate: ReportDate);


            MyTimer = new System.Timers.Timer();
            MyTimer.Elapsed += new ElapsedEventHandler(PeriodicCall);
            MyTimer.Interval = 30000;
            // And start it        
            MyTimer.Enabled = true;

            DataSet PythonOutput = IOUtilities.ExcelDataReader.LoadFile(DirectoryName + "/" + DirectoryExtension + "/ifs.xlsx");
            DataTable IfsSheet = PythonOutput.Tables["all"];
           
            AutoSpreaderList = new List<ttapiUtils.AutoSpreader>();
            AutoSpreaderDictionary = new Dictionary<string, ttapiUtils.AutoSpreader>();

            SummaryTable = new DataTable();
            SummaryTable.Columns.Add("Ticker", typeof(string));
            SummaryTable.Columns.Add("TickerHead", typeof(string));
            SummaryTable.Columns.Add("Bid", typeof(double));
            SummaryTable.Columns.Add("Ask", typeof(double));
            SummaryTable.Columns.Add("Mid", typeof(double));
            SummaryTable.Columns.Add("MidConverted", typeof(double));
            SummaryTable.Columns.Add("Tag", typeof(string));
            SummaryTable.Columns.Add("Alias", typeof(string));
            SummaryTable.Columns.Add("DescriptionString", typeof(string));
            
            DataColumn ValidPriceColumn = new DataColumn("ValidPrice", typeof(bool));
            ValidPriceColumn.DefaultValue = false;
            SummaryTable.Columns.Add(ValidPriceColumn);

            DataColumn WorkingOrdersColumn = new DataColumn("WorkingOrders", typeof(int));
            WorkingOrdersColumn.DefaultValue = 0;
            SummaryTable.Columns.Add(WorkingOrdersColumn);

            DataColumn WorkingOrderKeyColumn = new DataColumn("WorkingOrderKey", typeof(string));
            WorkingOrderKeyColumn.DefaultValue = "";
            SummaryTable.Columns.Add(WorkingOrderKeyColumn);

            DataColumn PositionColumn = new DataColumn("Position", typeof(int));
            PositionColumn.DefaultValue = 0;
            SummaryTable.Columns.Add(PositionColumn);

            DataColumn IndicatorLowerLimitColumn = new DataColumn("LowerLimit", typeof(double));
            IndicatorLowerLimitColumn.DefaultValue = Double.MinValue;
            SummaryTable.Columns.Add(IndicatorLowerLimitColumn);

            DataColumn IndicatorUpperLimitColumn = new DataColumn("UpperLimit", typeof(double));
            IndicatorUpperLimitColumn.DefaultValue = Double.MaxValue;
            SummaryTable.Columns.Add(IndicatorUpperLimitColumn);

            DataTable newDataTable = IfsSheet.AsEnumerable()
                   .OrderBy(r => r.Field<string>("spread_description"))
                   .ThenByDescending(r => r.Field<double>("min_volume"))
                   .CopyToDataTable();

            LiquidSpreads = newDataTable.AsEnumerable().GroupBy(r => r["spread_description"]).Select(w => w.First()).CopyToDataTable();
            LiquidSpreads = LiquidSpreads.Select("spread_description='W_KW'").CopyToDataTable();

            for (int i = 0; i < LiquidSpreads.Rows.Count; i++)
            {
                List<string> TickerList = new List<string> { LiquidSpreads.Rows[i].Field<string>("Contract1"), LiquidSpreads.Rows[i].Field<string>("Contract2"), LiquidSpreads.Rows[i].Field<string>("Contract3") };
                TickerList.RemoveAll(item => item == null);
                AutoSpreaderList.Add(new ttapiUtils.AutoSpreader(dbTickerList: TickerList, payUpTicks: 2));

                DataRow Row = SummaryTable.NewRow();
                Row["Ticker"] = AutoSpreaderList[i].AutoSpreaderName;
                Row["TickerHead"] = LiquidSpreads.Rows[i].Field<string>("spread_description");
                Row["LowerLimit"] = LiquidSpreads.Rows[i].Field<double>("maSpreadLow");
                Row["UpperLimit"] = LiquidSpreads.Rows[i].Field<double>("maSpreadHigh");
                Row["Tag"] = "ifs_2_" + i.ToString();
                Row["Alias"] = LiquidSpreads.Rows[i].Field<string>("spread_description") + "_ifs_2_" + DateTime.Now.ToString("MMM-yyyy");
                Row["DescriptionString"] = "strategy_class=ifs2&betsize=1000";

                AutoSpreaderDictionary.Add(AutoSpreaderList[i].AutoSpreaderName, AutoSpreaderList[i]);

                SummaryTable.Rows.Add(Row);

            }

            ASENameList = DataAnalysis.DataTableFunctions.GetColumnAsList<string>(dataTableInput: SummaryTable, columnName: "Ticker");
            TagList = DataAnalysis.DataTableFunctions.GetColumnAsList<string>(dataTableInput: SummaryTable, columnName: "Tag");

            TTAPISubs = new ttapiUtils.Subscription(m_username, m_password);
            TTAPISubs.AutoSpreaderList = AutoSpreaderList;
            TTAPISubs.AsuUpdateList = new List<EventHandler<AuthenticationStatusUpdateEventArgs>> { TTAPISubs.StartASESubscriptions };
            TTAPISubs.priceUpdatedEventHandler = m_ps_FieldsUpdated;
            TTAPISubs.orderFilledEventHandler = m_ts_OrderFilled;
            TTAPISubs.OrderDeletedEventHandler = OrderDeleted;


            TimeSeriesObj = new DataAnalysis.TimeSeries(StartTime, EndTime, ASENameList.ToArray(), minInterval);

        }

        private void PeriodicCall(object source,ElapsedEventArgs e) 
        {
            DateTime TimeStamp = DateTime.Now;
            DateTime TimeStampFull = new DateTime(TodayDate.Year, TodayDate.Month, TodayDate.Day, TimeStamp.Hour, TimeStamp.Minute, 0);

            int RowIndx = 0;
            OpportunityZone OpZone;
            PositionZone PosZone;
            int Qty = 1;

            DataRow SelectedRow = SummaryTable.Rows[RowIndx];
            ttapiUtils.AutoSpreader SelectedAutospreader = AutoSpreaderDictionary[SelectedRow.Field<string>("Ticker")];
            Instrument SelectedInstrument = SelectedAutospreader.AutoSpreaderInstrument;

            if (SelectedRow.Field<bool>("ValidPrice"))
            {
                TimeSeriesObj.updateValues(newPrice: SelectedRow.Field<double>("MidConverted"),
                    newDateTime: TimeStampFull, instrument: SelectedRow.Field<string>("Ticker"));

                double IndicatorValue = TechnicalSignals.MovingAverage.CalculateTimeSeriesMovingAverage(timeSeries: TimeSeriesObj.Data,
                    columnName: SelectedRow.Field<string>("Ticker"), dateTimePoint: TimeStampFull, numObs: 40);

                double Spread = SelectedRow.Field<double>("MidConverted") - IndicatorValue;

                if (Spread > SelectedRow.Field<double>("UpperLimit"))
                { OpZone = OpportunityZone.Zone1;}
                else if ((Spread <= SelectedRow.Field<double>("UpperLimit")) && (Spread > 0))
                { OpZone = OpportunityZone.Zone2; }
                else if ((Spread <= 0) && (Spread > SelectedRow.Field<double>("LowerLimit")))
                { OpZone = OpportunityZone.Zone3; }
                else if (Spread < SelectedRow.Field<double>("LowerLimit"))
                { OpZone = OpportunityZone.Zone4; }
                else
                { OpZone = OpportunityZone.Zone0; }

                if ((SelectedRow.Field<int>("Position") == 0)&&(SelectedRow.Field<int>("WorkingOrders") == 0))
                { PosZone = PositionZone.None; }
                else if ((SelectedRow.Field<int>("Position") > 0)&&(SelectedRow.Field<int>("WorkingOrders") == 0))
                { PosZone = PositionZone.Long; }
                else if ((SelectedRow.Field<int>("Position") < 0) && (SelectedRow.Field<int>("WorkingOrders") == 0))
                { PosZone = PositionZone.Short; }
                else if ((SelectedRow.Field<int>("Position") == 0) && (SelectedRow.Field<int>("WorkingOrders") > 0))
                { PosZone = PositionZone.WorkingEntryLong; }
                else if ((SelectedRow.Field<int>("Position") == 0) && (SelectedRow.Field<int>("WorkingOrders") < 0))
                { PosZone = PositionZone.WorkingEntryShort; }
                else if ((SelectedRow.Field<int>("Position") > 0)&&(SelectedRow.Field<int>("WorkingOrders") < 0))
                { PosZone = PositionZone.WorkingExitLong; }
                else if ((SelectedRow.Field<int>("Position") < 0) && (SelectedRow.Field<int>("WorkingOrders") > 0))
                { PosZone = PositionZone.WorkingEntryShort; }
                else
                { PosZone = PositionZone.Undefined; }


                //New Short Position
                if ((OpZone == OpportunityZone.Zone1)&& (PosZone == PositionZone.None))
                {
                    IFSLogger.Log(SelectedRow.Field<string>("Alias") + " entering a short position for " + SelectedRow.Field<double>("Mid"));
                    SelectedRow["WorkingOrders"] = -Qty;

                    SelectedRow["WorkingOrderKey"] = ttapiUtils.Trade.SendAutospreaderOrder(instrument: SelectedInstrument, instrumentDetails: SelectedInstrument.InstrumentDetails, autoSpreader: SelectedAutospreader,
                                qty: -Qty, price: (decimal)SelectedRow.Field<double>("Mid"), orderTag: SelectedRow.Field<string>("Tag"), logger: IFSLogger);
                }

                //New Long Position 
                else if ((OpZone == OpportunityZone.Zone4) && (PosZone == PositionZone.None))
                {
                    IFSLogger.Log(SelectedRow.Field<string>("Alias") + " entering a long position for " + SelectedRow.Field<double>("Mid"));
                    SelectedRow["WorkingOrders"] = Qty;

                    SelectedRow["WorkingOrderKey"] = ttapiUtils.Trade.SendAutospreaderOrder(instrument: SelectedInstrument, instrumentDetails: SelectedInstrument.InstrumentDetails, autoSpreader: SelectedAutospreader,
                                qty: Qty, price: (decimal)SelectedRow.Field<double>("Mid"), orderTag: SelectedRow.Field<string>("Tag"), logger: IFSLogger);
                }

                //Adjusting Short Open Position
                else if ((OpZone == OpportunityZone.Zone1) && (PosZone == PositionZone.WorkingEntryShort))
  
                {
                    IFSLogger.Log(SelectedRow.Field<string>("Alias") + " adjusting short open order price to " + SelectedRow.Field<double>("Mid"));
                    ttapiUtils.Trade.ChangeAutospreaderOrder(orderKey: SelectedRow.Field<string>("WorkingOrderKey"),
                        price: (decimal)SelectedRow.Field<double>("Mid"), autoSpreader: SelectedAutospreader, instrument: SelectedInstrument, logger: IFSLogger);
                }

                //Adjusting Long Open Position 
                else if ((OpZone == OpportunityZone.Zone4) && (PosZone == PositionZone.WorkingEntryLong))
                {
                    IFSLogger.Log(SelectedRow.Field<string>("Alias") + " adjusting long open order price to " + SelectedRow.Field<double>("Mid"));
                    ttapiUtils.Trade.ChangeAutospreaderOrder(orderKey: SelectedRow.Field<string>("WorkingOrderKey"),
                        price: (decimal)SelectedRow.Field<double>("Mid"), autoSpreader: SelectedAutospreader, instrument: SelectedInstrument, logger: IFSLogger);
                }

                //Closing Short Position
                else if (((OpZone == OpportunityZone.Zone3) | (OpZone == OpportunityZone.Zone4))&&(PosZone==PositionZone.Short))
                {
                    IFSLogger.Log(SelectedRow.Field<string>("Alias") + " has normalized. Closing position for " + SelectedRow.Field<double>("Mid"));
                    SelectedRow["WorkingOrders"] = -SelectedRow.Field<int>("Position");

                    SelectedRow["WorkingOrderKey"] = ttapiUtils.Trade.SendAutospreaderOrder(instrument: SelectedInstrument, instrumentDetails: SelectedInstrument.InstrumentDetails, autoSpreader: SelectedAutospreader,
                                qty: -SelectedRow.Field<int>("Position"), price: (decimal)SelectedRow.Field<double>("Mid"), orderTag: SelectedRow.Field<string>("Tag"), logger: IFSLogger);
                }

                //Closing Long Position
                else if (((OpZone == OpportunityZone.Zone1) | (OpZone == OpportunityZone.Zone2)) && (PosZone == PositionZone.Long))
                {
                    IFSLogger.Log(SelectedRow.Field<string>("Alias") + " has normalized. Closing position for " + SelectedRow.Field<double>("Mid"));
                    SelectedRow["WorkingOrders"] = -SelectedRow.Field<int>("Position");

                    SelectedRow["WorkingOrderKey"] = ttapiUtils.Trade.SendAutospreaderOrder(instrument: SelectedInstrument, instrumentDetails: SelectedInstrument.InstrumentDetails, autoSpreader: SelectedAutospreader,
                                qty: -SelectedRow.Field<int>("Position"), price: (decimal)SelectedRow.Field<double>("Mid"), orderTag: SelectedRow.Field<string>("Tag"), logger: IFSLogger);
                }


                // Adjusting Short Close Position
                else if (((OpZone == OpportunityZone.Zone3) | (OpZone == OpportunityZone.Zone4))&&(PosZone==PositionZone.WorkingExitShort))
                {
                    IFSLogger.Log(SelectedRow.Field<string>("Alias") + " adjusting short close order price to " + SelectedRow.Field<double>("Mid"));
                    ttapiUtils.Trade.ChangeAutospreaderOrder(orderKey: SelectedRow.Field<string>("WorkingOrderKey"),
                        price: (decimal)SelectedRow.Field<double>("Mid"), autoSpreader: SelectedAutospreader, instrument: SelectedInstrument, logger: IFSLogger);
                }

                // Adjusting Long Close Position
                else if (((OpZone == OpportunityZone.Zone1) | (OpZone == OpportunityZone.Zone2)) && (PosZone == PositionZone.WorkingExitLong))
                {
                    IFSLogger.Log(SelectedRow.Field<string>("Alias") + " adjusting long close order price to " + SelectedRow.Field<double>("Mid"));
                    ttapiUtils.Trade.ChangeAutospreaderOrder(orderKey: SelectedRow.Field<string>("WorkingOrderKey"),
                        price: (decimal)SelectedRow.Field<double>("Mid"), autoSpreader: SelectedAutospreader, instrument: SelectedInstrument, logger: IFSLogger);
                }


                //Canceling Short Entry Order
                else if ((OpZone!= OpportunityZone.Zone1)&&(PosZone==PositionZone.WorkingEntryShort))

                {
                    IFSLogger.Log(SelectedRow.Field<string>("Alias") + " has normalized canceling working order");
                    
                    bool cancelSuccess = ttapiUtils.Trade.CancelAutospreaderOrder(orderKey: SelectedRow.Field<string>("WorkingOrderKey"),
                        autoSpreader: SelectedAutospreader, logger: IFSLogger);

                    if (cancelSuccess)
                    {
                        SelectedRow["WorkingOrders"] = 0;
                    }
                }

                     //Canceling Long Entry Order
                else if ((OpZone!= OpportunityZone.Zone4) && (PosZone == PositionZone.WorkingEntryLong))
                {
                    IFSLogger.Log(SelectedRow.Field<string>("Alias") + " has normalized canceling working order");

                    bool cancelSuccess = ttapiUtils.Trade.CancelAutospreaderOrder(orderKey: SelectedRow.Field<string>("WorkingOrderKey"),
                        autoSpreader: SelectedAutospreader, logger: IFSLogger);

                    if (cancelSuccess)
                    {
                        SelectedRow["WorkingOrders"] = 0;
                    }
                }

                 //Canceling Short Exit Order
                else if ((OpZone != OpportunityZone.Zone3) && (OpZone != OpportunityZone.Zone4) && (PosZone == PositionZone.WorkingExitShort))
                {
                    IFSLogger.Log(SelectedRow.Field<string>("Alias") + " has de-normalized canceling working order");

                    bool cancelSuccess = ttapiUtils.Trade.CancelAutospreaderOrder(orderKey: SelectedRow.Field<string>("WorkingOrderKey"),
                        autoSpreader: SelectedAutospreader, logger: IFSLogger);

                    if (cancelSuccess)
                    {
                        SelectedRow["WorkingOrders"] = 0;
                    }
                }

                 //Canceling Long Exit Order
                else if ((OpZone != OpportunityZone.Zone1) && (OpZone != OpportunityZone.Zone2) && (PosZone == PositionZone.WorkingExitLong))
                {
                    IFSLogger.Log(SelectedRow.Field<string>("Alias") + " has de-normalized canceling working order");

                    bool cancelSuccess = ttapiUtils.Trade.CancelAutospreaderOrder(orderKey: SelectedRow.Field<string>("WorkingOrderKey"),
                        autoSpreader: SelectedAutospreader, logger: IFSLogger);

                    if (cancelSuccess)
                    {
                        SelectedRow["WorkingOrders"] = 0;
                    }
                }


             
                Console.WriteLine(DateTime.Now + " Recorded Price: " + SummaryTable.Rows[RowIndx].Field<double>("MidConverted"));
                Console.WriteLine(DateTime.Now + " Spread: " + (SummaryTable.Rows[RowIndx].Field<double>("MidConverted") - IndicatorValue));

            }


        }

        void m_ps_FieldsUpdated(object sender, FieldsUpdatedEventArgs e)
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

        void m_ts_OrderFilled(object sender, OrderFilledEventArgs e)
        {
            string Tag = e.Fill.OrderTag;
            int RowIndex = TagList.IndexOf(Tag);
            DataRow RelevantRow = SummaryTable.Rows[RowIndex];

            string Alias = RelevantRow.Field<string>("Alias");

            if (!TA.Strategy.CheckIfStrategyExist(alias: Alias, conn: conn))
            {
                TA.Strategy.GenerateDbStrategyFromAlias(alias: Alias, descriptionString: RelevantRow.Field<string>("DescriptionString"), conn: conn);
            }

            int TradeQuantity;

            if (e.Fill.BuySell == BuySell.Buy)
            {
                TradeQuantity = e.Fill.Quantity;
            }
            else
            {
                TradeQuantity = -e.Fill.Quantity;
            }

            if (e.Fill.IsAutospreaderSyntheticFill)
            {
                RelevantRow["Position"] = RelevantRow.Field<int>("Position") + TradeQuantity;
                RelevantRow["WorkingOrders"] = RelevantRow.Field<int>("WorkingOrders") - TradeQuantity;
                RelevantRow["WorkingOrderKey"] = "";
                return;
            }

            Instrument Inst = AutoSpreaderList[RowIndex].InstrumentDictionary[e.Fill.InstrumentKey];

            string TickerDB = TA.TickerConverters.ConvertFromTTAPIFields2DB(Inst.Product.ToString(), Inst.Name.ToString());
            string TickerHead = TA.TickerheadConverters.ConvertFromTT2DB(Inst.Product.ToString());
            
            TA.Strategy.LoadTrade2Strategy(ticker: TickerDB, trade_price: (decimal)TA.PriceConverters.FromTT2DB(ttPrice: Convert.ToDecimal(e.Fill.MatchPrice.ToString()),
                tickerHead: TickerHead), trade_quantity: TradeQuantity, instrument: "F", alias: Alias, conn: conn);

            IFSLogger.Log(Alias + ": " + TradeQuantity.ToString() + " " + TickerDB + " for " + TA.PriceConverters.FromTT2DB(ttPrice: Convert.ToDecimal(e.Fill.MatchPrice.ToString()),
                tickerHead: TickerHead).ToString());

        }

        void OrderDeleted(object sender, OrderDeletedEventArgs e)
        {

            string Tag = e.OldOrder.OrderTag;
            int RowIndex = TagList.IndexOf(Tag);
            DataRow RelevantRow = SummaryTable.Rows[RowIndex];
            RelevantRow["WorkingOrderKey"] = "";
        }


        public void Dispose()
        {
            TTAPISubs.Dispose();
        }
    }
}
