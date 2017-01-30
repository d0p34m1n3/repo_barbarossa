using DatabaseConnection;
using Excel;
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

namespace FuturesButterfly
{
    public class FuturesButterflyTTAPI: IDisposable
    {
        public ttapiUtils.Subscription TTAPISubs;
        private string m_username = "";
        private string m_password = "";
        Logger ButterflyLogger;
        MySqlConnection conn;
        List<ttapiUtils.AutoSpreader> AutoSpreaderList;
        Dictionary<string,ttapiUtils.AutoSpreader> AutoSpreaderDictionary;
        Dictionary<string, string> Alias2AutoSpreaderDictionary;
        Dictionary<string, string> TagDictionary;
        List<string> SpreadList;
        DataTable SpreadTable;
        DataTable PriceTable;
        DataTable HistoricPriceTable;
        List<string> PriceTableSymbols;
        List<string> PriceTableSymbolsReceived;
        Dictionary<string,DataTable> StrategyPositionDictionary;
        string TickerDB;
        DataTable ButterfliesStopSheet;
        DataTable ButterfliesSheetFiltered;
        DateTime LogTime1;
        System.Timers.Timer PeriodicTimer;
        


        public FuturesButterflyTTAPI(string u, string p)
        {

            m_username = u;
            m_password = p;

            mysql connection = new mysql();
            conn = connection.conn;

            string OutputFolder = TA.DirectoryNames.GetDirectoryName("daily");
            StreamWriter LogFile = new StreamWriter(OutputFolder + "/FuturesButterfly.txt", true);
            ButterflyLogger = new Logger(LogFile);

            AutoSpreaderList = new List<ttapiUtils.AutoSpreader>();
            AutoSpreaderDictionary = new Dictionary<string, ttapiUtils.AutoSpreader>();
            Alias2AutoSpreaderDictionary = new Dictionary<string, string>();
            TagDictionary = new Dictionary<string, string>();
            LogTime1 = DateTime.MinValue;

            PriceTableSymbolsReceived = new List<string>();
            StrategyPositionDictionary = new Dictionary<string, DataTable>();

            TTAPISubs = new ttapiUtils.Subscription(m_username, m_password);
            SpreadList = new List<string>();

            DateTime ReportDate = CalendarUtilities.BusinessDays.GetBusinessDayShifted(shiftInDays: -1);
            DateTime TodayDate = DateTime.Now.Date;

            

            ButterflyLogger.SW.WriteLine();
            ButterflyLogger.Log("NOTES FOR " + TodayDate.ToString("MM/dd/yyyy"));
            ButterflyLogger.Log(new String('-', 20));

            string DirectoryName = TA.DirectoryNames.GetDirectoryName(ext: "ta");
            string ButterflyOutputDirectoryName = TA.DirectoryNames.GetDirectoryName(ext: "futuresButterflyOutput");
            string DirectoryExtension = TA.DirectoryNames.GetDirectoryExtension(directoryDate: ReportDate);

            DataSet FollowupOutput = IOUtilities.ExcelDataReader.LoadFile(DirectoryName + "/" + DirectoryExtension + "/followup.xlsx");
            DataTable ButterfliesFollowupSheet = FollowupOutput.Tables["butterflies"];

            DataSet ButterfliesOutput = IOUtilities.ExcelDataReader.LoadFile(ButterflyOutputDirectoryName + "/" + DirectoryExtension + "/butterflies.xlsx");
            DataTable ButterfliesSheet = ButterfliesOutput.Tables["good"];

            PeriodicTimer = new System.Timers.Timer();
            PeriodicTimer.Elapsed += new ElapsedEventHandler(PeriodicCall);
            PeriodicTimer.Interval = 30000;
            // And start it        
            PeriodicTimer.Enabled = true;

            

            DataColumn SizeColumn = new DataColumn("Size", typeof(double));
            SizeColumn.DefaultValue = 0;
            ButterfliesSheet.Columns.Add(SizeColumn);

            ButterfliesSheet.Columns.Add("Theme", typeof(string));
            
            DataColumn ExistingThemePositionColumn = new DataColumn("ExistingThemePosition", typeof(double));
            ExistingThemePositionColumn.DefaultValue = 0;
            ButterfliesSheet.Columns.Add(ExistingThemePositionColumn);

            ButterfliesSheet.Columns.Add("SlackFromTheme", typeof(double));

            

            

            
            for (int i = 0; i < ButterfliesFollowupSheet.Rows.Count; i++)
            {
                StrategyUtilities.PositionManagerOutput PositionManagerOut = PositionManager.GetFuturesButterflyPosition(alias: ButterfliesFollowupSheet.Rows[i].Field<string>("Alias"), 
                    asOfDate: TodayDate, conn: conn);
                if (PositionManagerOut.CorrectPositionQ)
                {
                    StrategyPositionDictionary.Add(ButterfliesFollowupSheet.Rows[i].Field<string>("Alias"), PositionManagerOut.SortedPosition);
                }
            }


            DataTable ButterfliesFollowupSheetAdjusted = RiskManager.CalculateAdjustedDownside(dataTableInput: ButterfliesFollowupSheet, newTradesTable: ButterfliesSheet,
                strategyPositionDictionary: StrategyPositionDictionary);

            DataTable RiskAcrossTickerhead = RiskManager.AggregateRiskAcrossTickerHead(dataTableInput: ButterfliesFollowupSheetAdjusted);

            Dictionary<string, double> RiskAcrossTheme = RiskManager.GetRiskAcrossTheme(dataTableInput: ButterfliesFollowupSheetAdjusted, 
                strategyPositionDictionary: StrategyPositionDictionary);



            ButterfliesSheetFiltered = Filtering.GetFilteredNewTrades(butterfliesSheet: ButterfliesSheet, riskAcrossTickerhead: RiskAcrossTickerhead,riskAcrossTheme:RiskAcrossTheme);

            DataColumn ButterflyQuantityColumn = new DataColumn("ButterflyQuantity", typeof(int));
            ButterflyQuantityColumn.DefaultValue = 0;
            ButterfliesSheetFiltered.Columns.Add(ButterflyQuantityColumn);

            DataColumn Spread1QuantityColumn = new DataColumn("Spread1Quantity", typeof(int));
            Spread1QuantityColumn.DefaultValue = 0;
            ButterfliesSheetFiltered.Columns.Add(Spread1QuantityColumn);

            DataColumn Spread2QuantityColumn = new DataColumn("Spread2Quantity", typeof(int));
            Spread2QuantityColumn.DefaultValue = 0;
            ButterfliesSheetFiltered.Columns.Add(Spread2QuantityColumn);

            ButterfliesSheetFiltered.Columns.Add("Alias", typeof(string));

            PriceTable = new DataTable();
            PriceTable.Columns.Add("Ticker", typeof(string));
            PriceTable.Columns.Add("TickerHead", typeof(string));
            PriceTable.Columns.Add("IsAutoSpreaderQ", typeof(bool));
            
            DataColumn BidPriceColumn = new DataColumn("BidPrice", typeof(double));
            BidPriceColumn.DefaultValue = Double.NaN;
            PriceTable.Columns.Add(BidPriceColumn);

            DataColumn AskPriceColumn = new DataColumn("AskPrice", typeof(double));
            AskPriceColumn.DefaultValue = Double.NaN;
            PriceTable.Columns.Add(AskPriceColumn);

            PriceTable.Columns.Add("BidQ", typeof(int));
            PriceTable.Columns.Add("AskQ", typeof(int));



            DataColumn ValidPriceColumn = new DataColumn("ValidPriceQ", typeof(bool));
            ValidPriceColumn.DefaultValue = false;
            PriceTable.Columns.Add(ValidPriceColumn);

            for (int i = 0; i < ButterfliesSheetFiltered.Rows.Count; i++)
            {
                DataRow SelectedRow = ButterfliesSheetFiltered.Rows[i];
                string Alias = "";
                string Ticker1 = SelectedRow.Field<string>("ticker1");
                string Ticker2 = SelectedRow.Field<string>("ticker2");
                string Ticker3 = SelectedRow.Field<string>("ticker3");


                if (SelectedRow.Field<double>("z1") < 0)
                {
                    double FirstSpreadQuantity = Math.Round(SelectedRow.Field<double>("Size") / Math.Abs(SelectedRow.Field<double>("Downside")));
                    double SecondSpreadQuantity = Math.Round(FirstSpreadQuantity * SelectedRow.Field<double>("second_spread_weight_1"));

                    if (FirstSpreadQuantity<SecondSpreadQuantity)
                    {
                        SelectedRow["ButterflyQuantity"] = (int)FirstSpreadQuantity;
                        SelectedRow["Spread2Quantity"] = -(int)(SecondSpreadQuantity - FirstSpreadQuantity);
                    }
                    else if (FirstSpreadQuantity > SecondSpreadQuantity)
                    {
                        SelectedRow["ButterflyQuantity"] = (int)SecondSpreadQuantity;
                        SelectedRow["Spread1Quantity"] = (int)(FirstSpreadQuantity - SecondSpreadQuantity);
                    }

                    else
                    {
                        ButterfliesSheetFiltered.Rows[i]["ButterflyQuantity"] = (int)FirstSpreadQuantity;
                    }

                    Alias = SelectedRow.Field<string>("tickerHead") + Ticker1.Substring(Ticker1.Count() - 5, 5) + Ticker2.Substring(Ticker2.Count() - 5, 5) +
                        Ticker2.Substring(Ticker2.Count() - 5, 5) + Ticker3.Substring(Ticker3.Count() - 5, 5);

                       
                }
                else if (ButterfliesSheetFiltered.Rows[i].Field<double>("z1")>0)
                {
                    double FirstSpreadQuantity = Math.Round(ButterfliesSheetFiltered.Rows[i].Field<double>("Size") / Math.Abs(ButterfliesSheetFiltered.Rows[i].Field<double>("Upside")));
                    double SecondSpreadQuantity = Math.Round(FirstSpreadQuantity * ButterfliesSheetFiltered.Rows[i].Field<double>("second_spread_weight_1"));

                    if (FirstSpreadQuantity < SecondSpreadQuantity)
                    {
                        SelectedRow["ButterflyQuantity"] = -(int)FirstSpreadQuantity;
                        SelectedRow["Spread2Quantity"] = (int)(SecondSpreadQuantity - FirstSpreadQuantity);
                    }
                    else if (FirstSpreadQuantity > SecondSpreadQuantity)
                    {
                        SelectedRow["ButterflyQuantity"] = -(int)SecondSpreadQuantity;
                        SelectedRow["Spread1Quantity"] = -(int)(FirstSpreadQuantity - SecondSpreadQuantity);
                    }

                    else
                    {
                        SelectedRow["ButterflyQuantity"] = (int)FirstSpreadQuantity;
                    }

                    Alias = SelectedRow.Field<string>("tickerHead") + Ticker2.Substring(Ticker2.Count() - 5, 5) + Ticker3.Substring(Ticker3.Count() - 5, 5) +
                        Ticker1.Substring(Ticker1.Count() - 5, 5) + Ticker2.Substring(Ticker2.Count() - 5, 5);
                        
                }

                SelectedRow["Alias"] = Alias;

                List<string> TickerList = new List<string> { Ticker1 + "-" + Ticker2,
                                                             Ticker2 + "-" + Ticker3};

                ttapiUtils.AutoSpreader As = new ttapiUtils.AutoSpreader(dbTickerList: TickerList, payUpTicks: 2);

                SpreadList.AddRange(TickerList);
                AutoSpreaderList.Add(As);
                AutoSpreaderDictionary.Add(As.AutoSpreaderName, As);
                Alias2AutoSpreaderDictionary.Add(Alias, As.AutoSpreaderName);
                TagDictionary.Add(As.AutoSpreaderName, "fut_but_fol_" + i);

                DataRow PriceRow = PriceTable.NewRow();
                PriceRow["Ticker"] = As.AutoSpreaderName;
                PriceRow["TickerHead"] = SelectedRow.Field<string>("tickerHead");
                PriceRow["IsAutoSpreaderQ"] = true;
                PriceTable.Rows.Add(PriceRow);


            }





            




            

            HistoricPriceTable = GetPrice.GetFuturesPrice.getFuturesPrice4Ticker(tickerHeadList: ContractUtilities.ContractMetaInfo.FuturesButterflyTickerheadList, 
                dateTimeFrom: ReportDate, dateTimeTo: ReportDate, conn: conn);

            DataRow [] StopList = ButterfliesFollowupSheet.Select("Recommendation='STOP'");

            if (StopList.Length>0)
            {
                ButterfliesStopSheet = StopList.CopyToDataTable();
                ButterfliesStopSheet.Columns.Add("PnlMid",typeof(double));
                ButterfliesStopSheet.Columns.Add("PnlWorst", typeof(double));
                ButterfliesStopSheet.Columns.Add("ButterflyMidPrice", typeof(double));
                ButterfliesStopSheet.Columns.Add("ButterflyWorstPrice", typeof(double));
                ButterfliesStopSheet.Columns.Add("ButterflyQuantity", typeof(int));
                ButterfliesStopSheet.Columns.Add("SpreadMidPrice", typeof(double));
                ButterfliesStopSheet.Columns.Add("SpreadWorstPrice", typeof(double));
                ButterfliesStopSheet.Columns.Add("SpreadQuantity", typeof(int));

                DataColumn WorkingButterflyOrdersColumn = new DataColumn("WorkingButterflyOrders", typeof(int));
                WorkingButterflyOrdersColumn.DefaultValue = 0;
                ButterfliesStopSheet.Columns.Add(WorkingButterflyOrdersColumn);

                DataColumn WorkingSpreadOrdersColumn = new DataColumn("WorkingSpreadOrders", typeof(int));
                WorkingSpreadOrdersColumn.DefaultValue = 0;
                ButterfliesStopSheet.Columns.Add(WorkingSpreadOrdersColumn);

                for (int i = 0; i < StopList.Length; i++)
                {
                    string Alias = StopList[i].Field<string>("Alias");
                    StrategyUtilities.PositionManagerOutput PositionManagerOut = PositionManager.GetFuturesButterflyPosition(alias: Alias, asOfDate: TodayDate, conn: conn);
                    DataTable StrategyPosition = PositionManagerOut.SortedPosition;

                    StrategyPosition.Columns.Add("ClosePrice", typeof(double));

                    for (int j = 0; j < StrategyPosition.Rows.Count; j++)
                    {
                        DataRow SelectedHistRow = HistoricPriceTable.Select("ticker='" + StrategyPosition.Rows[j].Field<string>("Ticker") + "'")[0];
                        StrategyPosition.Rows[j]["ClosePrice"] = SelectedHistRow.Field<decimal>("close_price");
                    }


                    List<string> TickerList = new List<string> { StrategyPosition.Rows[0].Field<string>("Ticker") + "-" + StrategyPosition.Rows[1].Field<string>("Ticker"),
                                                             StrategyPosition.Rows[1].Field<string>("Ticker") + "-" + StrategyPosition.Rows[2].Field<string>("Ticker")};
                    StrategyPositionDictionary[Alias] = StrategyPosition;

                    SpreadList.AddRange(TickerList);

                    if (PositionManagerOut.CorrectPositionQ)
                    {
                        
                        ttapiUtils.AutoSpreader As = new ttapiUtils.AutoSpreader(dbTickerList: TickerList, payUpTicks: 2);

                        AutoSpreaderList.Add(As);
                        AutoSpreaderDictionary.Add(As.AutoSpreaderName, As);
                        Alias2AutoSpreaderDictionary.Add(Alias,As.AutoSpreaderName);
                        TagDictionary.Add(As.AutoSpreaderName, "fut_but_fol_" + (i + ButterfliesSheetFiltered.Rows.Count));


                        DataRow PriceRow = PriceTable.NewRow();
                        PriceRow["Ticker"] = As.AutoSpreaderName;
                        PriceRow["TickerHead"] = StrategyPosition.Rows[0].Field<string>("TickerHead");
                        PriceRow["IsAutoSpreaderQ"] = true;
                        PriceTable.Rows.Add(PriceRow);

                    }
                    else
                    {
                        ButterflyLogger.Log("Check " + Alias + " ! Position may be incorrect.");
                    }
                }


                SpreadList = SpreadList.Distinct().ToList();

                SpreadTable = new DataTable();
                SpreadTable.Columns.Add("Ticker", typeof(string));
                SpreadTable.Columns.Add("IsSpreadQ", typeof(bool));

                for (int i = 0; i < SpreadList.Count; i++)
                {
                    DataRow SpreadRow = SpreadTable.NewRow();
                    SpreadRow["Ticker"] = SpreadList[i];
                    SpreadRow["IsSpreadQ"] = true;
                    SpreadTable.Rows.Add(SpreadRow);

                    DataRow PriceRow = PriceTable.NewRow();
                    PriceRow["Ticker"] = SpreadList[i];
                    string[] SpreadAsArray = SpreadList[i].Split(new char[] { '-' });
                    PriceRow["TickerHead"] = ContractUtilities.ContractMetaInfo.GetContractSpecs(SpreadAsArray[0]).tickerHead;
                    PriceRow["IsAutoSpreaderQ"] = false;
                    PriceTable.Rows.Add(PriceRow);
                }
    
            }

            

            

            PriceTableSymbols = DataAnalysis.DataTableFunctions.GetColumnAsList<string>(dataTableInput: PriceTable, columnName: "Ticker");


            TTAPISubs.TickerTable = SpreadTable;
            TTAPISubs.AutoSpreaderList = AutoSpreaderList;
            TTAPISubs.AsuUpdateList = new List<EventHandler<AuthenticationStatusUpdateEventArgs>> { TTAPISubs.StartASESubscriptions,TTAPISubs.startInstrumentLookupSubscriptionsFromDataTable };
            //TTAPISubs.AsuUpdateList = new List<EventHandler<AuthenticationStatusUpdateEventArgs>> {TTAPISubs.startInstrumentLookupSubscriptionsFromDataTable };
            TTAPISubs.ilsUpdateList = new List<EventHandler<InstrumentLookupSubscriptionEventArgs>> { TTAPISubs.startPriceSubscriptions, TTAPISubs.startTradeSubscriptions };
            //TTAPISubs.ilsUpdateList = new List<EventHandler<InstrumentLookupSubscriptionEventArgs>> { TTAPISubs.startPriceSubscriptions};
            TTAPISubs.priceUpdatedEventHandler = m_ps_FieldsUpdated;
            TTAPISubs.orderFilledEventHandler = m_ts_OrderFilled;

        }

        private void PeriodicCall(object source, ElapsedEventArgs e)
        {
            ButterfliesStopSheet = CalculateStrategyPnls(StrategyTableInput: ButterfliesStopSheet);

            DataRow[] Ready2CloseRows = ButterfliesStopSheet.Select("PnlMid>0");

            if (Ready2CloseRows.Count() == 0)
            {
                if (DateTime.UtcNow > LogTime1.AddMinutes(5))
                {
                    ButterflyLogger.Log("No trades to close");
                    LogTime1 = DateTime.UtcNow;
                }

            }

           
                for (int i = 0; i < Ready2CloseRows.Count(); i++)
                {

                    string Alias = Ready2CloseRows[i].Field<string>("Alias");

                    if (Ready2CloseRows[i].Field<int>("ButterflyQuantity") + Ready2CloseRows[i].Field<int>("WorkingButterflyOrders") != 0)
                    {

                        Ready2CloseRows[i]["WorkingButterflyOrders"] = -Ready2CloseRows[i].Field<int>("ButterflyQuantity");

                        ButterflyLogger.Log(Ready2CloseRows[i].Field<string>("Alias") + " will be closed");
                        ButterflyLogger.Log("Intraday pnl: " + Ready2CloseRows[i].Field<double>("PnlMid"));
                        ButterflyLogger.Log("Number of Butterflies: " + Ready2CloseRows[i].Field<int>("ButterflyQuantity"));
                        ButterflyLogger.Log("Number of Extra Spreads: " + Ready2CloseRows[i].Field<int>("SpreadQuantity"));
                        Console.Write("Do you agree? (Y/N): ");
                        string Decision = Console.ReadLine();
                        Decision = Decision.ToUpper();

                        if (Decision == "Y")
                        {

                            string AutoSpreaderName = Alias2AutoSpreaderDictionary[Alias];
                            ttapiUtils.AutoSpreader AutoSpreader = AutoSpreaderDictionary[AutoSpreaderName];
                            string OrderTag = TagDictionary[AutoSpreaderName];

                            string emre = "merve";
                            ttapiUtils.Trade.SendAutospreaderOrder(instrument: AutoSpreader.AutoSpreaderInstrument, instrumentDetails: AutoSpreader.AutoSpreaderInstrument.InstrumentDetails,
                                autoSpreader: AutoSpreader, qty: Ready2CloseRows[i].Field<int>("WorkingButterflyOrders"),
                                 price: (decimal)Ready2CloseRows[i].Field<double>("ButterflyMidPrice"), orderTag: OrderTag, logger: ButterflyLogger);


                        }
                        else if (Decision == "N")
                        {
                            ButterflyLogger.Log("You have chosen to keep " + Ready2CloseRows[i].Field<string>("Alias"));
                        }
                    }
                }
        }

        void m_ps_FieldsUpdated(object sender, FieldsUpdatedEventArgs e)
        {
            Price BidPrice = e.Fields.GetDirectBidPriceField().Value;
            Price AskPrice = e.Fields.GetDirectAskPriceField().Value;

            

            if ((!BidPrice.IsValid)||(!AskPrice.IsValid))
            {
                return;
            }

            string InstrumentName = e.Fields.Instrument.Name.ToString();

            if (e.Fields.Instrument.Product.FormattedName=="Autospreader")
            {
                TickerDB = e.Fields.Instrument.Name;
            }
            else
            {
                TickerDB = TA.TickerConverters.ConvertFromTTAPIFields2DB(e.Fields.Instrument.Product.ToString(), e.Fields.Instrument.Name.ToString());
            }

            int RowIndex = PriceTableSymbols.IndexOf(TickerDB);
            
            double BidPriceDb = BidPrice.ToDouble();
            double AskPriceDb = AskPrice.ToDouble();

            PriceTable.Rows[RowIndex]["BidPrice"] = BidPriceDb;
            PriceTable.Rows[RowIndex]["AskPrice"] = AskPriceDb;
            PriceTable.Rows[RowIndex]["BidQ"] = e.Fields.GetDirectBidQuantityField().Value.ToInt();
            PriceTable.Rows[RowIndex]["AskQ"] = e.Fields.GetDirectAskQuantityField().Value.ToInt();
            PriceTable.Rows[RowIndex]["ValidPriceQ"] = true;

            PriceTableSymbolsReceived.Add(TickerDB);

            List<string> wuhu = PriceTableSymbols.Except(PriceTableSymbolsReceived).ToList();
             
        }

        DataTable CalculateStrategyPnls(DataTable StrategyTableInput)
        {

            


            for (int i = 0; i < StrategyTableInput.Rows.Count; i++)
            {
                string Alias = StrategyTableInput.Rows[i].Field<string>("Alias");
                DataTable StrategyPosition = StrategyPositionDictionary[Alias];
                DataRow SelectedRow;

                double ContractMultiplier = ContractUtilities.ContractMetaInfo.ContractMultiplier[StrategyPosition.Rows[0].Field<string>("TickerHead")];

                

                int NumButterflies = Convert.ToInt32(Math.Sign(StrategyPosition.Rows[0].Field<double>("Qty")) *
                    Math.Min(Math.Abs(StrategyPosition.Rows[0].Field<double>("Qty")), Math.Abs(StrategyPosition.Rows[2].Field<double>("Qty"))));

                double ButterflyPriceClose = StrategyPosition.Rows[0].Field<double>("ClosePrice")
                                          - 2 * StrategyPosition.Rows[1].Field<double>("ClosePrice")
                                            + StrategyPosition.Rows[2].Field<double>("ClosePrice");

                SelectedRow = PriceTable.Select("Ticker='" + StrategyPosition.Rows[0].Field<string>("Ticker") + "-" + StrategyPosition.Rows[1].Field<string>("Ticker") + "_" +
                                               StrategyPosition.Rows[1].Field<string>("Ticker") + "-" + StrategyPosition.Rows[2].Field<string>("Ticker") + "'")[0];

                double ButterflyMidPrice = (SelectedRow.Field<double>("BidPrice") + SelectedRow.Field<double>("AskPrice")) / 2;
                StrategyTableInput.Rows[i]["ButterflyMidPrice"] = ButterflyMidPrice;
                double ButterflyWorstPrice = double.NaN;

                if (!Double.IsNaN(ButterflyMidPrice))
                {
                    ButterflyMidPrice = (double)TA.PriceConverters.FromTT2DB(ttPrice: (decimal)ButterflyMidPrice, tickerHead: StrategyPosition.Rows[0].Field<string>("TickerHead"));
                }

                if (NumButterflies > 0)
                {
                    ButterflyWorstPrice = SelectedRow.Field<double>("BidPrice");
                }
                else if (NumButterflies < 0)
                {
                    ButterflyWorstPrice = SelectedRow.Field<double>("AskPrice");
                }

                StrategyTableInput.Rows[i]["ButterflyWorstPrice"] = ButterflyWorstPrice;

                if (!Double.IsNaN(ButterflyWorstPrice))
                {
                    ButterflyWorstPrice = (double)TA.PriceConverters.FromTT2DB(ttPrice: (decimal)ButterflyWorstPrice, tickerHead: StrategyPosition.Rows[0].Field<string>("TickerHead"));
                }
                
                StrategyTableInput.Rows[i]["ButterflyQuantity"] = NumButterflies;
                double ButterflyMidPnl = ContractMultiplier * NumButterflies * (ButterflyMidPrice - ButterflyPriceClose);
                double ButterflyWorstPnl = ContractMultiplier * NumButterflies * (ButterflyWorstPrice - ButterflyPriceClose);

                string AdditionalTicker = "";
                int AdditionalQuantity = 0;
                double AdditionalTickerClosePrice = Double.NaN;

                if ((Math.Abs(NumButterflies) < Math.Abs(StrategyPosition.Rows[0].Field<double>("Qty"))))
                {
                    AdditionalTicker = StrategyPosition.Rows[0].Field<string>("Ticker") + "-" + StrategyPosition.Rows[1].Field<string>("Ticker");
                    AdditionalQuantity = (int)StrategyPosition.Rows[0].Field<double>("Qty") - NumButterflies;
                    AdditionalTickerClosePrice = StrategyPosition.Rows[0].Field<double>("ClosePrice") - StrategyPosition.Rows[1].Field<double>("ClosePrice");

                }
                else if ((Math.Abs(NumButterflies) < Math.Abs(StrategyPosition.Rows[2].Field<double>("Qty"))))
                {
                    AdditionalTicker = StrategyPosition.Rows[1].Field<string>("Ticker") + "-" + StrategyPosition.Rows[2].Field<string>("Ticker");
                    AdditionalQuantity = -((int)StrategyPosition.Rows[2].Field<double>("Qty") - NumButterflies);
                    AdditionalTickerClosePrice = StrategyPosition.Rows[1].Field<double>("ClosePrice") - StrategyPosition.Rows[2].Field<double>("ClosePrice");
                }

                double AdditionalTickerMidPnl = 0;
                double AdditionalTickerWorstPnl = 0;

                if (AdditionalQuantity != 0)
                {
                    SelectedRow = PriceTable.Select("Ticker='" + StrategyPosition.Rows[0].Field<string>("Ticker") + "-" + StrategyPosition.Rows[1].Field<string>("Ticker") + "'")[0];
                    double AdditionalTickerMidPrice = (SelectedRow.Field<double>("BidPrice") + SelectedRow.Field<double>("AskPrice")) / 2;
                    StrategyTableInput.Rows[i]["SpreadMidPrice"] = AdditionalTickerMidPrice;

                    AdditionalTickerMidPrice = (double)TA.PriceConverters.FromTT2DB(ttPrice: (decimal)AdditionalTickerMidPrice, tickerHead: StrategyPosition.Rows[0].Field<string>("TickerHead"));

                    AdditionalTickerMidPnl = ContractMultiplier * AdditionalQuantity * (AdditionalTickerMidPrice - AdditionalTickerClosePrice);

                    double AdditionalTickerWorstPrice = double.NaN;

                    if (AdditionalQuantity > 0)
                    {
                        AdditionalTickerWorstPrice = SelectedRow.Field<double>("BidPrice");
                    }
                    else if (AdditionalQuantity < 0)
                    {
                        AdditionalTickerWorstPrice = SelectedRow.Field<double>("AskPrice");
                    }

                    StrategyTableInput.Rows[i]["SpreadWorstPrice"] = AdditionalTickerWorstPrice;
                    AdditionalTickerWorstPrice = (double)TA.PriceConverters.FromTT2DB(ttPrice: (decimal)AdditionalTickerWorstPrice, tickerHead: StrategyPosition.Rows[0].Field<string>("TickerHead"));

                    AdditionalTickerWorstPnl = ContractMultiplier * AdditionalQuantity * (AdditionalTickerWorstPrice - AdditionalTickerClosePrice);

                }

                StrategyTableInput.Rows[i]["SpreadQuantity"] = AdditionalQuantity;
                StrategyTableInput.Rows[i]["PnlMid"] = Math.Round(ButterflyMidPnl + AdditionalTickerMidPnl);
                StrategyTableInput.Rows[i]["PnlWorst"] = Math.Round(ButterflyWorstPnl + AdditionalTickerWorstPnl);
                

            }

            return StrategyTableInput;
        }

        void m_ts_OrderFilled(object sender, OrderFilledEventArgs e)
        {
            string emre = "merve";
        }


        public void Dispose()
        {
            TTAPISubs.Dispose();
        }
    }
}
