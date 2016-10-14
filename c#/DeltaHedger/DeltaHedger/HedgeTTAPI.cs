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
using TradingTechnologies.TTAPI.Tradebook;

namespace DeltaHedger
{
    public class HedgeTTAPI: IDisposable
    {
        public ttapiUtils.Subscription TTAPISubs;
        private string m_username = "";
        private string m_password = "";
        public Dictionary<InstrumentKey, InstrumentLookupSubscription> IlsDictionary;
        List<EventHandler<InstrumentLookupSubscriptionEventArgs>> ilsUpdateList;
        DataTable PriceData;
        bool PricesReceivedQ;
        DataTable UnderlyingTickerTable;
        DataTable SortedHistoricPriceTable;
        DataTable NetHedgeTable;
        List<string> NetHedgeTickerList;
        List<string> UnderlyingTickerList;
        mysql connection;
        MySqlConnection conn;
        Logger DeltaLogger;
        string TickerDB;
        DateTime DateTo;
        private InstrumentTradeSubscription Ts;
        string DeltaStrategyAlias;
        

        public HedgeTTAPI(string u, string p)
        {

            m_username = u;
            m_password = p;

            PricesReceivedQ = false;

            connection = new mysql();
            conn = connection.conn;

            UnderlyingTickerTable = Underlying.GetUnderlying2QueryTable(conn: conn);

            List<string> TickerHeadList = DataAnalysis.DataTableFunctions.GetColumnAsList<string>(dataTableInput: UnderlyingTickerTable, columnName: "TickerHead", uniqueQ: true);

            DateTo = CalendarUtilities.BusinessDays.GetBusinessDayShifted(shiftInDays:-1);
            DateTime DateFrom = CalendarUtilities.BusinessDays.GetBusinessDayShifted(shiftInDays: -10);

            DataTable HistoricPriceTable = GetPrice.GetFuturesPrice.getFuturesPrice4Ticker(tickerHeadList: TickerHeadList, dateTimeFrom: DateFrom, dateTimeTo: DateTo, conn: conn);

            SortedHistoricPriceTable = HistoricPriceTable.AsEnumerable()
                   .OrderBy(r => r.Field<string>("ticker"))
                   .ThenBy(r => r.Field<DateTime>("price_date"))
                   .CopyToDataTable();

            TTAPISubs = new ttapiUtils.Subscription(m_username, m_password);
            IlsDictionary = TTAPISubs.IlsDictionary;
            TTAPISubs.TickerTable = UnderlyingTickerTable;
            ilsUpdateList = new List<EventHandler<InstrumentLookupSubscriptionEventArgs>> { TTAPISubs.startPriceSubscriptions, TTAPISubs.startTradeSubscriptions };
            TTAPISubs.ilsUpdateList = ilsUpdateList;
            TTAPISubs.asu_update = TTAPISubs.startInstrumentLookupSubscriptionsFromDataTable;
            TTAPISubs.priceUpdatedEventHandler = GetBidAsk;
            TTAPISubs.orderFilledEventHandler = OrderFilledEventHandler;

            PriceData = new DataTable();
            PriceData.Columns.Add("Ticker", typeof(string));
            PriceData.Columns.Add("TickerHead", typeof(string));
            PriceData.Columns.Add("TickerClass", typeof(string));
            PriceData.Columns.Add("ContINDX", typeof(int));
            PriceData.Columns.Add("IsSpreadQ", typeof(bool));
           
            PriceData.Columns.Add("BidPrice", typeof(decimal));
            PriceData.Columns.Add("AskPrice", typeof(decimal));
            PriceData.Columns.Add("MidPrice", typeof(decimal));
            PriceData.Columns.Add("SpreadCost", typeof(decimal));

            foreach (DataRow item in UnderlyingTickerTable.Rows)
            {
                DataRow Row = PriceData.NewRow();
                Row["Ticker"] = item.Field<string>("Ticker");
                Row["TickerHead"] = item.Field<string>("TickerHead");
                Row["TickerClass"] = item.Field<string>("TickerClass");
                Row["ContINDX"] = item.Field<int>("ContINDX");
                Row["IsSpreadQ"] = item.Field<bool>("IsSpreadQ");
                PriceData.Rows.Add(Row);
            }

            PriceData = DataAnalysis.DataTableFunctions.Sort(dataTableInput: PriceData, columnList: new string[] { "TickerHead", "ContINDX" });
            UnderlyingTickerList = DataAnalysis.DataTableFunctions.GetColumnAsList<string>(dataTableInput: PriceData, columnName: "Ticker", uniqueQ: true);

            string OutputFolder = TA.DirectoryNames.GetDirectoryName("daily");
            StreamWriter LogFile = new StreamWriter(OutputFolder + "/Delta.txt", true);
            DeltaLogger = new Logger(LogFile);
            DeltaStrategyAlias = HedgeStrategies.GetActiveDeltaStrategy(conn: conn);



        }

        void GetBidAsk(object sender, FieldsUpdatedEventArgs e)
        {
            if (e.Error == null)
            {
                
                    string InstrumentName = e.Fields.Instrument.Name.ToString();

                    TickerDB = TA.TickerConverters.ConvertFromTTAPIFields2DB(e.Fields.Instrument.Product.ToString(), e.Fields.Instrument.Name.ToString());
                    string TickerHead = TA.TickerheadConverters.ConvertFromTT2DB(e.Fields.Instrument.Product.ToString());

                    UpdatePriceData(ticker: TickerDB, tickerHead: TickerHead, e: e);

                    TA.ttapiTicker TTAPITicker = TA.TickerConverters.ConvertFromDbTicker2ttapiTicker(dbTicker: TickerDB, productType: e.Fields.Instrument.Product.Type.ToString());

                    InstrumentKey IKey = new InstrumentKey(new ProductKey(e.Fields.Instrument.Product.Market.Key, e.Fields.Instrument.Product.Type, e.Fields.Instrument.Product.Name), TTAPITicker.SeriesKey);
                    Ts = TTAPISubs.TsDictionary[e.Fields.Instrument.Key];

                    if (IlsDictionary.ContainsKey(IKey))
                    {
                        for (int i = 0; i < ilsUpdateList.Count; i++)
                        {
                            IlsDictionary[IKey].Update -= ilsUpdateList[i];
                        }

                        IlsDictionary[IKey].Dispose();
                        IlsDictionary[IKey] = null;
                        IlsDictionary.Remove(IKey);
                        TTAPISubs.IlsDictionary = IlsDictionary;
                    }

                    
                    if ((IlsDictionary.Count == 0)&(!PricesReceivedQ))
                    {
                        PricesReceivedQ = true;
                        //HedgeStrategies.HedgeStrategiesAgainstDelta(conn: conn, priceTable: PriceData);




                        DataTable StdMoves = CalculateStdMoves(priceData: PriceData);
                        DataTable HedgeTable = GenerateHedgeTable(conn: conn);
                        NetHedgeTable = CalculateUrgency(hedgeTable: HedgeTable);

                        List<string> TickerHeads2Report = DataAnalysis.DataTableFunctions.GetColumnAsList<string>(dataTableInput: StdMoves, columnName: "TickerHead", uniqueQ: true);

                        

                        foreach (string TickerHead2Report in TickerHeads2Report)
                        {
                            DeltaLogger.Log(new String('-', 20));
                            DataRow[] SelectedRows = StdMoves.Select("TickerHead='" + TickerHead2Report + "'");

                            foreach (DataRow Row in SelectedRows)
                            {
                                DeltaLogger.Log(Row.Field<string>("Ticker") + " percent change: " + Row.Field<decimal>("StdChange"));
                            }

                            DataRow[] SelectedHedgeRows = NetHedgeTable.Select("TickerHead='" + TickerHead2Report + "'");

                            foreach (DataRow Row in SelectedHedgeRows)
                            {
                                DeltaLogger.Log(Row.Field<int>("Hedge") + "  " + Row.Field<string>("Ticker") + " with urgency " + Row.Field<decimal>("Urgency"));
                            }


                        }


                        DataColumn WorkingOrdersColumn = new DataColumn("WorkingOrders", typeof(int));
                        WorkingOrdersColumn.DefaultValue = 0;
                        NetHedgeTable.Columns.Add(WorkingOrdersColumn);
                        NetHedgeTickerList = DataAnalysis.DataTableFunctions.GetColumnAsList<string>(dataTableInput: NetHedgeTable, columnName: "Ticker");



                        }

                if (PricesReceivedQ)
                {
                    int RowIndex = NetHedgeTickerList.IndexOf(TickerDB);

                    if ((RowIndex < 0) || (NetHedgeTable.Rows[RowIndex].Field<int>("WorkingOrders") != 0))
                    {
                        return;
                    }

                    Decimal MidPrice = (Convert.ToDecimal(e.Fields.GetDirectBidPriceField().FormattedValue) + Convert.ToDecimal(e.Fields.GetDirectAskPriceField().FormattedValue)) / 2;


                    NetHedgeTable.Rows[RowIndex]["WorkingOrders"] = NetHedgeTable.Rows[RowIndex].Field<int>("Hedge");

                    if (NetHedgeTable.Rows[RowIndex].Field<int>("Hedge")>0)
                    {
                        ttapiUtils.Trade.SendLimitOrder(instrument: e.Fields.Instrument, price: e.Fields.GetBestBidPriceField().Value, 
                            qty: NetHedgeTable.Rows[RowIndex].Field<int>("Hedge"), ttapisubs:TTAPISubs,orderTag:"DeltaHedge");
                    }
                    else if  (NetHedgeTable.Rows[RowIndex].Field<int>("Hedge")<0)
                    {
                        ttapiUtils.Trade.SendLimitOrder(instrument:e.Fields.Instrument, price:e.Fields.GetBestAskPriceField().Value,
                            qty: NetHedgeTable.Rows[RowIndex].Field<int>("Hedge"), ttapisubs: TTAPISubs, orderTag: "DeltaHedge");
                    }

                    string FilledTicker = TA.TickerConverters.ConvertFromTTAPIFields2DB(e.Fields.Instrument.Product.ToString(), e.Fields.Instrument.Name.ToString());
                    string wuhu = TA.TickerheadConverters.ConvertFromTT2DB(e.Fields.Instrument.Product.ToString());
                    
                    

                        
            }
                        

                        //liquidContractList = new ContractUtilities.ContractList(instrumentList);




                
            }
            else
            {
                if (e.Error.IsRecoverableError == false)
                {
                    Console.WriteLine("Unrecoverable price subscription error: {0}", e.Error.Message);
                    //Dispose();
                }
            }
        }

        void UpdatePriceData(string ticker,string tickerHead,FieldsUpdatedEventArgs e)
        {
            int RowIndex = UnderlyingTickerList.IndexOf(ticker);

            PriceData.Rows[RowIndex]["BidPrice"] =
                       TA.PriceConverters.FromTT2DB(ttPrice: Convert.ToDecimal(e.Fields.GetDirectBidPriceField().FormattedValue), tickerHead: tickerHead);
            PriceData.Rows[RowIndex]["AskPrice"] =
                TA.PriceConverters.FromTT2DB(ttPrice: Convert.ToDecimal(e.Fields.GetDirectAskPriceField().FormattedValue), tickerHead: tickerHead);

            PriceData.Rows[RowIndex]["MidPrice"] = (PriceData.Rows[RowIndex].Field<decimal>("AskPrice") +
                PriceData.Rows[RowIndex].Field<decimal>("BidPrice")) / 2;

            PriceData.Rows[RowIndex]["SpreadCost"] = (PriceData.Rows[RowIndex].Field<decimal>("AskPrice") -
                PriceData.Rows[RowIndex].Field<decimal>("BidPrice")) * (decimal)ContractUtilities.ContractMetaInfo.ContractMultiplier[tickerHead];
        }

        DataTable GenerateHedgeTable(MySqlConnection conn)
        {
            string DeltaStrategy = HedgeStrategies.GetDeltaStrategyList(conn: conn).Last();
            //string DeltaStrategy = HedgeStrategies.GetDeltaStrategyList(conn: conn).First();

            DataTable DeltaNetPosition = TA.Strategy.GetNetPosition(alias: DeltaStrategy, conn: conn);
            DeltaNetPosition.Columns.Add("TickerHead", typeof(string));
            DeltaNetPosition.Columns.Add("TickerClass", typeof(string));
            DeltaNetPosition.Columns.Add("ContINDX", typeof(int));

            for (int i = 0; i < DeltaNetPosition.Rows.Count; i++)
            {
                ContractUtilities.ContractSpecs Cs = ContractUtilities.ContractMetaInfo.GetContractSpecs(ticker: DeltaNetPosition.Rows[i].Field<string>("Ticker"));
                DeltaNetPosition.Rows[i]["TickerHead"] = Cs.tickerHead;
                DeltaNetPosition.Rows[i]["TickerClass"] = Cs.tickerClass;
                DeltaNetPosition.Rows[i]["ContINDX"] = Cs.contINDX;
            }


            DataRow[] FlatCurvePositions = (from x in DeltaNetPosition.AsEnumerable()
                                            where Underlying.FlatCurveTickerClassList.Any(b => x.Field<string>("TickerClass").Contains(b))
                                            select x).ToArray();

            DataRow[] NonFlatCurvePositions = (from x in DeltaNetPosition.AsEnumerable()
                                               where !Underlying.FlatCurveTickerClassList.Any(b => x.Field<string>("TickerClass").Contains(b))
                                               select x).ToArray();

            DataTable FlatCurveHedgeTable = new DataTable();
            FlatCurveHedgeTable.Columns.Add("Ticker", typeof(string));
            FlatCurveHedgeTable.Columns.Add("TickerHead", typeof(string));
            FlatCurveHedgeTable.Columns.Add("IsSpreadQ", typeof(bool));
            FlatCurveHedgeTable.Columns.Add("Hedge", typeof(int));

            DataTable NonFlatCurveHedgeTable = new DataTable();
            NonFlatCurveHedgeTable.Columns.Add("Ticker", typeof(string));
            NonFlatCurveHedgeTable.Columns.Add("TickerHead", typeof(string));
            NonFlatCurveHedgeTable.Columns.Add("IsSpreadQ", typeof(bool));
            NonFlatCurveHedgeTable.Columns.Add("Hedge", typeof(int));

            if (FlatCurvePositions.Count() > 0)
            {
                DataTable FlatCurveTable = FlatCurvePositions.CopyToDataTable();

                ContractUtilities.ContractList liquidContractList = new ContractUtilities.ContractList(FlatCurveTable.AsEnumerable().Select(s => s.Field<string>("TickerHead")).Distinct().ToArray());

                Dictionary<string, string> LiquidContractDictionary = new Dictionary<string, string>();

                foreach (string item in liquidContractList.dbTickerList)
                {
                    LiquidContractDictionary.Add(ContractUtilities.ContractMetaInfo.GetContractSpecs(item).tickerHead, item);
                }

                var grouped = FlatCurveTable.AsEnumerable().GroupBy(r => r["TickerHead"]).Select(w => new
                {
                    TickerHead = w.Key.ToString(),
                    Qty = w.Sum(r => decimal.Parse(r["Qty"].ToString())),
                }).ToList();


                for (int i = 0; i < grouped.Count; i++)
                {
                    FlatCurveHedgeTable.Rows.Add();
                    FlatCurveHedgeTable.Rows[i]["Ticker"] = LiquidContractDictionary[grouped[i].TickerHead];
                    FlatCurveHedgeTable.Rows[i]["TickerHead"] = grouped[i].TickerHead;
                    FlatCurveHedgeTable.Rows[i]["IsSpreadQ"] = false;
                    FlatCurveHedgeTable.Rows[i]["Hedge"] = Math.Round(-grouped[i].Qty);
                }
            }

            if (NonFlatCurvePositions.Count() > 0)
            {

                DataTable SortedNonFlatCurvePositions = NonFlatCurvePositions.AsEnumerable()
       .OrderBy(r => r.Field<string>("TickerHead"))
       .ThenBy(r => r.Field<int>("contINDX"))
       .CopyToDataTable();

                List<string> TickerHeadList = SortedNonFlatCurvePositions.AsEnumerable().Select(s => s.Field<string>("TickerHead")).Distinct().ToList();

                for (int i = 0; i < TickerHeadList.Count(); i++)
                {
                    DataTable TickerHeadPositions = SortedNonFlatCurvePositions.Select("TickerHead='" + TickerHeadList[i] + "'").CopyToDataTable();

                    if (TickerHeadPositions.Rows.Count == 1)
                    {
                        DataRow Row = NonFlatCurveHedgeTable.NewRow();
                        NonFlatCurveHedgeTable.Rows.Add();
                        Row["Ticker"] = TickerHeadPositions.Rows[0].Field<string>("Ticker");
                        Row["TickerHead"] = TickerHeadList[i];
                        Row["IsSpreadQ"] = false;
                        Row["Hedge"] = Math.Round(-TickerHeadPositions.Rows[0].Field<double>("Qty"));
                        NonFlatCurveHedgeTable.Rows.Add(Row);
                        continue;
                    }

                    int sum = (int)Math.Round(-TickerHeadPositions.AsEnumerable().Sum(x => x.Field<double>("Qty")));

                    int MaxIndex = TickerHeadPositions.Rows.IndexOf(TickerHeadPositions.Select("Qty = MAX(Qty)")[0]);
                    int MinIndex = TickerHeadPositions.Rows.IndexOf(TickerHeadPositions.Select("Qty = MIN(Qty)")[0]);

                    if (sum > 0)
                    {
                        DataRow Row = NonFlatCurveHedgeTable.NewRow();
                        Row["Ticker"] = TickerHeadPositions.Rows[MinIndex].Field<string>("Ticker");
                        Row["TickerHead"] = TickerHeadList[i];
                        Row["IsSpreadQ"] = false;
                        Row["Hedge"] = sum;
                        NonFlatCurveHedgeTable.Rows.Add(Row);
                        TickerHeadPositions.Rows[MinIndex]["Qty"] = TickerHeadPositions.Rows[MinIndex].Field<double>("Qty") + sum;

                    }
                    else if (sum < 0)
                    {
                        DataRow Row = NonFlatCurveHedgeTable.NewRow();
                        Row["Ticker"] = TickerHeadPositions.Rows[MaxIndex].Field<string>("Ticker");
                        Row["TickerHead"] = TickerHeadList[i];
                        Row["IsSpreadQ"] = false;
                        Row["Hedge"] = sum;
                        NonFlatCurveHedgeTable.Rows.Add(Row);
                        TickerHeadPositions.Rows[MaxIndex]["Qty"] = TickerHeadPositions.Rows[MaxIndex].Field<double>("Qty") + sum;
                    }

                    bool PositionCleanedQ = false;

                    while (!PositionCleanedQ)
                    {

                        var OrderedRows = TickerHeadPositions.AsEnumerable().OrderByDescending(x => x["Qty"]);

                        MaxIndex = TickerHeadPositions.Rows.IndexOf(OrderedRows.First());
                        MinIndex = TickerHeadPositions.Rows.IndexOf(OrderedRows.Last());

                        PositionCleanedQ = !((TickerHeadPositions.Rows[MaxIndex].Field<double>("Qty") > 0.5) & (TickerHeadPositions.Rows[MinIndex].Field<double>("Qty") < -0.5));

                        if (PositionCleanedQ)
                        {
                            break;
                        }
                        int qty = -(int)Math.Round(Math.Min(TickerHeadPositions.Rows[MaxIndex].Field<double>("Qty"), -TickerHeadPositions.Rows[MinIndex].Field<double>("Qty")));

                        string SpreadTicker;
                        int HedgeQty;

                        if (TickerHeadPositions.Rows[MaxIndex].Field<int>("ContINDX") < TickerHeadPositions.Rows[MinIndex].Field<int>("ContINDX"))
                        {
                            SpreadTicker = TickerHeadPositions.Rows[MaxIndex].Field<string>("Ticker") + "-" + TickerHeadPositions.Rows[MinIndex].Field<string>("Ticker");
                            HedgeQty = qty;
                        }
                        else
                        {
                            HedgeQty = -qty;
                            SpreadTicker = TickerHeadPositions.Rows[MinIndex].Field<string>("Ticker") + "-" + TickerHeadPositions.Rows[MaxIndex].Field<string>("Ticker");
                        }

                        DataRow Row = NonFlatCurveHedgeTable.NewRow();
                        Row["Ticker"] = SpreadTicker;
                        Row["TickerHead"] = TickerHeadList[i];
                        Row["IsSpreadQ"] = true;
                        Row["Hedge"] = HedgeQty;
                        NonFlatCurveHedgeTable.Rows.Add(Row);

                        TickerHeadPositions.Rows[MaxIndex]["Qty"] = TickerHeadPositions.Rows[MaxIndex].Field<double>("Qty") + qty;
                        TickerHeadPositions.Rows[MinIndex]["Qty"] = TickerHeadPositions.Rows[MinIndex].Field<double>("Qty") - qty;
                    }
                }
            }

            FlatCurveHedgeTable.Merge(NonFlatCurveHedgeTable);
            return FlatCurveHedgeTable;
        }

        DataTable CalculateUrgency(DataTable hedgeTable)
        {
            

            DataRow[] NetHedgeRows = hedgeTable.Select("Hedge<>0");

            DataTable NetHedgeTable = null;

            if (NetHedgeRows.Length > 0)
            {
                NetHedgeTable = NetHedgeRows.CopyToDataTable();
                NetHedgeTable.Columns.Add("SpreadCost", typeof(decimal));
                NetHedgeTable.Columns.Add("Risk", typeof(decimal));
                NetHedgeTable.Columns.Add("Urgency", typeof(decimal));

                for (int i = 0; i < NetHedgeRows.Length; i++)
                {
                    string Ticker = NetHedgeTable.Rows[i].Field<string>("Ticker");
                    NetHedgeTable.Rows[i]["SpreadCost"] = PriceData.Rows[UnderlyingTickerList.IndexOf(Ticker)].Field<decimal>("SpreadCost");


                    List<string> TickerList = Ticker.Split(new char[] { '-' }).ToList();

                    List<DataTable> AlignedDataList = DataAnalysis.Align.GenerateAlignedTimeSeries(seperatorList: TickerList,
                        seperatorField: "ticker", alignerField: "price_date", dataTableInput: SortedHistoricPriceTable);

                    List<decimal> HistoricTimeSeries = new List<decimal>();

                    if (TickerList.Count == 1)
                    {
                        HistoricTimeSeries = AlignedDataList[0].AsEnumerable().Select(r => r.Field<decimal>("close_price")).ToList();
                    }

                    else if (TickerList.Count == 2)
                    {
                        for (int j = 0; j < AlignedDataList[0].Rows.Count; j++)
                        {
                            HistoricTimeSeries.Add(AlignedDataList[0].Rows[j].Field<decimal>("close_price") - AlignedDataList[1].Rows[j].Field<decimal>("close_price"));
                        }
                    }

                    double ContractMultiplier = ContractUtilities.ContractMetaInfo.ContractMultiplier[ContractUtilities.ContractMetaInfo.GetContractSpecs(TickerList[0]).tickerHead];

                    if (NetHedgeTable.Rows[i].Field<decimal>("SpreadCost") > 0)
                    {
                        NetHedgeTable.Rows[i]["Risk"] = ContractMultiplier * (double)(HistoricTimeSeries.Max() - HistoricTimeSeries.Min());
                        NetHedgeTable.Rows[i]["Urgency"] = NetHedgeTable.Rows[i].Field<decimal>("Risk") / NetHedgeTable.Rows[i].Field<decimal>("SpreadCost");
                    }
                    else
                    {
                        DeltaLogger.Log(hedgeTable.Rows[i].Field<string>("Ticker") + " has zero spread cost check the prices! ");
                    }
                }
            }
            return NetHedgeTable;    
        }

        DataTable CalculateStdMoves(DataTable priceData)
        {
            DataTable PriceDataUnderlying = priceData.Select("IsSpreadQ=false").CopyToDataTable();

            List<string> UniqueTickerHeadList = DataAnalysis.DataTableFunctions.GetColumnAsList<string>(dataTableInput: PriceDataUnderlying, columnName: "TickerHead", uniqueQ: true);

            DataTable StdMovesTable = new DataTable();
            StdMovesTable.Columns.Add("Ticker", typeof(string));
            StdMovesTable.Columns.Add("TickerHead", typeof(string));
            StdMovesTable.Columns.Add("Settle", typeof(decimal));
            StdMovesTable.Columns.Add("MidPrice", typeof(decimal));
            StdMovesTable.Columns.Add("AtmVol", typeof(decimal));
            StdMovesTable.Columns.Add("StdChange", typeof(decimal));

            foreach (string SelectedTickerHead in UniqueTickerHeadList)
            {
                if (Underlying.FlatCurveTickerClassList.Contains(ContractUtilities.ContractMetaInfo.tickerClassDict[SelectedTickerHead]))
                {
                    ContractUtilities.ContractList liquidContractList = new ContractUtilities.ContractList(settleDate: DateTo, instrumentList: new string[1] { SelectedTickerHead });

                    DataRow SelectedRow = PriceDataUnderlying.Select("Ticker='" + liquidContractList.dbTickerList[0] + "'")[0];

                    DataRow StdMoveRow = StdMovesTable.NewRow();
                    StdMoveRow["Ticker"] = SelectedRow.Field<string>("Ticker");
                    StdMoveRow["TickerHead"] = SelectedRow.Field<string>("TickerHead");
                    StdMoveRow["MidPrice"] = SelectedRow.Field<decimal>("MidPrice");

                    DataRow[] SettleDataRows = SortedHistoricPriceTable.Select("ticker='" + liquidContractList.dbTickerList[0] + "' and price_date=#" + DateTo.ToString("MM/dd/yyyy") + "#");

                    if (SettleDataRows.Count() > 0)
                    {
                        StdMoveRow["Settle"] = SettleDataRows[0].Field<decimal>("close_price");
                    }

                    DataTable VolTable = Signals.OptionSignals.GetOptionTickerIndicators(ticker: liquidContractList.dbTickerList[0], settleDate: DateTo, conn: conn, columnNames: new string[1] { "imp_vol" });
                    StdMoveRow["AtmVol"] = VolTable.Rows[0].Field<decimal>("imp_vol");

                    StdMoveRow["StdChange"] = Math.Round((StdMoveRow.Field<decimal>("MidPrice") - StdMoveRow.Field<decimal>("Settle")) / StdMoveRow.Field<decimal>("Settle") /
                        (StdMoveRow.Field<decimal>("AtmVol") / (decimal)(100 * Math.Sqrt(250))), 2);
                    StdMovesTable.Rows.Add(StdMoveRow);
                }

                else
                {
                    DataRow[] SelectedRows = PriceDataUnderlying.Select("TickerHead='" + SelectedTickerHead + "'");

                    foreach (DataRow Row in SelectedRows)
                    {
                        DataRow StdMoveRow = StdMovesTable.NewRow();
                        StdMoveRow["Ticker"] = Row.Field<string>("Ticker");
                        StdMoveRow["TickerHead"] = Row.Field<string>("TickerHead");
                        StdMoveRow["MidPrice"] = Row.Field<decimal>("MidPrice");

                        DataRow[] SettleDataRows = SortedHistoricPriceTable.Select("ticker='" + Row.Field<string>("Ticker") + "' and price_date=#" + DateTo.ToString("MM/dd/yyyy") + "#");

                        if (SettleDataRows.Count() > 0)
                        {
                            StdMoveRow["Settle"] = SettleDataRows[0].Field<decimal>("close_price");
                        }

                        DataTable VolTable = Signals.OptionSignals.GetOptionTickerIndicators(ticker: Row.Field<string>("Ticker"), settleDate: DateTo, conn: conn, columnNames: new string[1] { "imp_vol" });
                        StdMoveRow["AtmVol"] = VolTable.Rows[0].Field<decimal>("imp_vol");

                        StdMoveRow["StdChange"] = Math.Round((StdMoveRow.Field<decimal>("MidPrice") - StdMoveRow.Field<decimal>("Settle")) / StdMoveRow.Field<decimal>("Settle") /
                            (StdMoveRow.Field<decimal>("AtmVol") / (decimal)(100 * Math.Sqrt(250))), 2);
                        StdMovesTable.Rows.Add(StdMoveRow);
                    }
                }
            }
            return StdMovesTable;
        }

        void OrderFilledEventHandler(object sender, OrderFilledEventArgs e)
        {
            Instrument Inst = ((InstrumentTradeSubscription)sender).Instrument;
            string FilledTicker = TA.TickerConverters.ConvertFromTTAPIFields2DB(Inst.Product.ToString(), Inst.Name.ToString());
            string TickerHead = TA.TickerheadConverters.ConvertFromTT2DB(Inst.Product.ToString());
            int FilledQuantity;

            if (e.Fill.BuySell == BuySell.Buy)
            {
                FilledQuantity = e.Fill.Quantity;
            }
            else
            {
                FilledQuantity = -e.Fill.Quantity;
            }

            TA.Strategy.LoadTrade2Strategy(ticker: FilledTicker, trade_price: (decimal)TA.PriceConverters.FromTT2DB(ttPrice: Convert.ToDecimal(e.Fill.MatchPrice.ToString()),
                tickerHead: TickerHead), trade_quantity: FilledQuantity, instrument: "F", alias: DeltaStrategyAlias, conn: conn);

        }
            
        public void Dispose()
        {
            TTAPISubs.Dispose();
        }
    }
}
