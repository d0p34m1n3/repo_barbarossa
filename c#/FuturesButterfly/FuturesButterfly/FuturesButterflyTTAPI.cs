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
        Dictionary<string, string> AliasDictionary;
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
            AliasDictionary = new Dictionary<string, string>();
            TagDictionary = new Dictionary<string, string>();

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
            string DirectoryExtension = TA.DirectoryNames.GetDirectoryExtension(directoryDate: ReportDate);

            DataSet FollowupOutput = IOUtilities.ExcelDataReader.LoadFile(DirectoryName + "/" + DirectoryExtension + "/followup.xlsx");
            DataTable ButterfliesFollowupSheet = FollowupOutput.Tables["butterflies"];

            PriceTable = new DataTable();
            PriceTable.Columns.Add("Ticker", typeof(string));
            PriceTable.Columns.Add("TickerHead", typeof(string));
            PriceTable.Columns.Add("IsAutoSpreaderQ", typeof(bool));
            PriceTable.Columns.Add("BidPrice", typeof(double));
            PriceTable.Columns.Add("AskPrice", typeof(double));

            HistoricPriceTable = GetPrice.GetFuturesPrice.getFuturesPrice4Ticker(tickerHeadList: ContractUtilities.ContractMetaInfo.FuturesButterflyTickerheadList, 
                dateTimeFrom: ReportDate, dateTimeTo: ReportDate, conn: conn);

            DataRow [] StopList = ButterfliesFollowupSheet.Select("Recommendation='STOP'");

            if (StopList.Length>0)
            {
                ButterfliesStopSheet = StopList.CopyToDataTable();
                ButterfliesStopSheet.Columns.Add("PnlMid",typeof(double));
                ButterfliesStopSheet.Columns.Add("PnlWorst", typeof(double));

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
                    StrategyPositionDictionary.Add(Alias, StrategyPosition);

                    SpreadList.AddRange(TickerList);

                    if (PositionManagerOut.CorrectPositionQ)
                    {
                        bool wuhu = TickerList[0].Contains("-");
                        string[] TickerListAux = TickerList[0].Split('-');

                        ttapiUtils.AutoSpreader As = new ttapiUtils.AutoSpreader(dbTickerList: TickerList, payUpTicks: 2);

                        AutoSpreaderList.Add(As);
                        AutoSpreaderDictionary.Add(As.AutoSpreaderName, As);
                        AliasDictionary.Add(As.AutoSpreaderName, Alias);
                        TagDictionary.Add(As.AutoSpreaderName, "fut_but_fol" + i);


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

        void m_ps_FieldsUpdated(object sender, FieldsUpdatedEventArgs e)
        {
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
            

            Price BidPrice = e.Fields.GetDirectBidPriceField().Value;
            Price AskPrice = e.Fields.GetDirectAskPriceField().Value;

            double BidPriceDb = BidPrice.ToDouble();
            double AskPriceDb = AskPrice.ToDouble();

            PriceTable.Rows[RowIndex]["BidPrice"] = BidPriceDb;
            PriceTable.Rows[RowIndex]["AskPrice"] = AskPriceDb;

            PriceTableSymbolsReceived.Add(TickerDB);

           if (PriceTableSymbols.Except(PriceTableSymbolsReceived).ToList().Count==0)
           {
               ButterfliesStopSheet = CalculateStrategyPnls(StrategyTableInput: ButterfliesStopSheet);
               DataRow[] Ready2CloseRows = ButterfliesStopSheet.Select("PnlMid>0");

               if (Ready2CloseRows.Count()==0)
               {
                   ButterflyLogger.Log("No trades to close");
               }

               if (e.Fields.Instrument.Product.FormattedName == "Autospreader")
               {
                   string emre = "merve";
               }


              
           }
            
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
                double ButterflyWorstPrice = double.NaN;

                ButterflyMidPrice = (double)TA.PriceConverters.FromTT2DB(ttPrice: (decimal)ButterflyMidPrice, tickerHead: StrategyPosition.Rows[0].Field<string>("TickerHead"));

                if (NumButterflies > 0)
                {
                    ButterflyWorstPrice = SelectedRow.Field<double>("BidPrice");
                }
                else if (NumButterflies < 0)
                {
                    ButterflyWorstPrice = SelectedRow.Field<double>("AskPrice");
                }

                ButterflyWorstPrice = (double)TA.PriceConverters.FromTT2DB(ttPrice: (decimal)ButterflyWorstPrice, tickerHead: StrategyPosition.Rows[0].Field<string>("TickerHead"));

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

                    AdditionalTickerWorstPrice = (double)TA.PriceConverters.FromTT2DB(ttPrice: (decimal)AdditionalTickerWorstPrice, tickerHead: StrategyPosition.Rows[0].Field<string>("TickerHead"));

                    AdditionalTickerWorstPnl = ContractMultiplier * AdditionalQuantity * (AdditionalTickerWorstPrice - AdditionalTickerClosePrice);

                }

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
