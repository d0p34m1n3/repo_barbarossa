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
using TradingTechnologies.TTAPI.Tradebook;


namespace IntTechScalper
{
    class Algo:IDisposable
    {
        private string m_username;
        private string m_password;
        public ttapiUtils.Subscription TTAPISubs;
        ContractUtilities.ContractList LiquidContractList;
        List<string> DbTickerList;
        string[] InstrumentArray;
        DateTime StartTime;
        DateTime EndTime;
        DateTime MorningRangeStartTime;
        DateTime MorningRangeEndTime;
        DataAnalysis.CandleStick candleObj;
        DataTable CandleData;
        TechnicalSignals.StochasticOscillator StochasticObj;
        DataTable StochasticOscillator;
        string TickerDB;
        DateTime DateTimeNow;
        System.Timers.Timer TradeTimer;
        int MinInterval;
        DateTime LastProcessedCandleDatetime;
        bool MorningRangeCalculatedQ;
        Dictionary<string, double> MorningMinDictionary;
        Dictionary<string, double> MorningMaxDictionary;
        Dictionary<string, string> WorkingLongEntryOrderKey;
        Dictionary<string, string> WorkingLongExitOrderKey;
        Dictionary<string, string> WorkingShortEntryOrderKey;
        Dictionary<string, string> WorkingShortExitOrderKey;

        Dictionary<string, double> BidPriceDictionary;
        Dictionary<string, double> AskPriceDictionary;

        Portfolio.Position ScalperPosition;
        mysql connection;
        MySqlConnection conn;
        Logger ITSLogger;
        int MaxNumBets;
        int NumBets;

        public Algo(string u, string p)
        {
            m_username = u;
            m_password = p;

            connection = new mysql();
            conn = connection.conn;

            InstrumentArray = ContractUtilities.ContractMetaInfo.cmeFuturesTickerheadList.Union(ContractUtilities.ContractMetaInfo.iceFuturesTickerheadList).ToArray();

            List<string> InstrumentList = new List<string>(InstrumentArray);
            InstrumentList.Remove("ED");
            InstrumentList.Remove("TU");
            InstrumentArray = InstrumentList.ToArray();

            InstrumentArray = new string[] {"BP"};

            LiquidContractList = new ContractUtilities.ContractList(InstrumentArray);
            DbTickerList = LiquidContractList.dbTickerList;

            ScalperPosition = new Portfolio.Position(fullTickerList: DbTickerList);

            string OutputFolder = TA.DirectoryNames.GetDirectoryName("daily");
            ITSLogger = new Logger(new StreamWriter(OutputFolder + "/TS.txt", true));

            TradeTimer = new System.Timers.Timer();
            TradeTimer.Elapsed += new ElapsedEventHandler(PeriodicCall);
            TradeTimer.Interval = 10000;
            // And start it        
            TradeTimer.Enabled = true;

            DateTime ReferanceDate = DateTime.Today;

            ITSLogger.Log("NOTES FOR " + ReferanceDate.ToString("MM/dd/yyyy"));
            ITSLogger.Log(new String('-', 20));

            MinInterval = 1;   //30
            MaxNumBets = 5;
            NumBets = 0;

            StartTime = new DateTime(ReferanceDate.Year, ReferanceDate.Month, ReferanceDate.Day, 8, 30, 0);   //830
            EndTime = new DateTime(ReferanceDate.Year, ReferanceDate.Month, ReferanceDate.Day, 16, 0, 0);    //900

            MorningRangeStartTime = StartTime;
            MorningRangeEndTime = new DateTime(ReferanceDate.Year, ReferanceDate.Month, ReferanceDate.Day, 9, 0, 0);
            MorningRangeCalculatedQ = false;

            MorningMinDictionary = new Dictionary<string, double>();
            MorningMaxDictionary = new Dictionary<string, double>();
            WorkingLongEntryOrderKey = new Dictionary<string, string>();
            WorkingLongExitOrderKey = new Dictionary<string, string>();
            WorkingShortEntryOrderKey = new Dictionary<string, string>();
            WorkingShortExitOrderKey = new Dictionary<string, string>();
            BidPriceDictionary = new Dictionary<string, double>();
            AskPriceDictionary = new Dictionary<string, double>();

            for (int i = 0; i < DbTickerList.Count; i++)
            {
                BidPriceDictionary.Add(DbTickerList[i], double.NaN);
                AskPriceDictionary.Add(DbTickerList[i], double.NaN);

                WorkingLongEntryOrderKey.Add(DbTickerList[i], "");
                WorkingLongExitOrderKey.Add(DbTickerList[i], "");
                WorkingShortEntryOrderKey.Add(DbTickerList[i], "");
                WorkingShortExitOrderKey.Add(DbTickerList[i], "");

            }

            LastProcessedCandleDatetime = StartTime;

            candleObj = new DataAnalysis.CandleStick(StartTime, EndTime, DbTickerList.ToArray(), MinInterval);
            CandleData = candleObj.data;

            StochasticObj = new TechnicalSignals.StochasticOscillator(startDate: StartTime.AddMinutes(MinInterval), endDate: EndTime, instrumentList: DbTickerList.ToArray(),
                minInterval: MinInterval, lookBack1: 5, lookBack2: 3, lookBack3: 3);
            StochasticOscillator = StochasticObj.Data;

            TTAPISubs = new ttapiUtils.Subscription(m_username, m_password);

            List<EventHandler<InstrumentLookupSubscriptionEventArgs>> IlsUpdateList = new List<EventHandler<InstrumentLookupSubscriptionEventArgs>>();
            IlsUpdateList.Add(TTAPISubs.startPriceSubscriptions);
            IlsUpdateList.Add(TTAPISubs.startTradeSubscriptions);

            TTAPISubs.ilsUpdateList = IlsUpdateList;
            TTAPISubs.dbTickerList = DbTickerList;
            TTAPISubs.priceUpdatedEventHandler = m_ps_FieldsUpdated;
            TTAPISubs.orderFilledEventHandler = OrderFilledLogic;
            TTAPISubs.AsuUpdateList = new List<EventHandler<AuthenticationStatusUpdateEventArgs>> { TTAPISubs.startInstrumentLookupSubscriptions };

        }

        private void PeriodicCall(object source, ElapsedEventArgs e)
        {
            DateTime CurrentTime = DateTime.Now;
            //int MinutesFromCandleClose = CurrentTime.Minute%MinInterval;
            int SecondsFromCandleClose = CurrentTime.Second;
            DateTime CandleDatetime = new DateTime(CurrentTime.Year, CurrentTime.Month, CurrentTime.Day, CurrentTime.Hour, CurrentTime.Minute, 0);

            if (CandleDatetime!=LastProcessedCandleDatetime)
            {
                
                //CandleDatetime = CandleDatetime.AddMinutes(-MinutesFromCandleClose);
                DateTime PrevCandleDatetime = CandleDatetime.AddMinutes(-MinInterval);

                
                    LastProcessedCandleDatetime = CandleDatetime;

                    //Console.WriteLine(CandleDatetime);
                    for (int i = 0; i < DbTickerList.Count; i++)
                    {
                        StochasticObj.UpdateValues(newDateTime: CandleDatetime, candleStickTable: CandleData, instrument: DbTickerList[i]);
                    }
                

               /* if ((!MorningRangeCalculatedQ)&(CandleDatetime>MorningRangeEndTime))
                {
                    DataTable SelectedCandleStickData = CandleData.Select("start>= #" + MorningRangeStartTime.ToString() + "# AND end<= #" + MorningRangeEndTime.ToString() + " # ").CopyToDataTable();
                    MorningRangeCalculatedQ = true;

                    for (int i = 0; i < DbTickerList.Count; i++)
                    {
                        double RangeMax = Convert.ToDouble(SelectedCandleStickData.Compute("Max(" + DbTickerList[i] + "_high" + ")", ""));
                        double RangeMin = Convert.ToDouble(SelectedCandleStickData.Compute("Min(" + DbTickerList[i] + "_low" + ")", ""));

                        MorningMinDictionary.Add(DbTickerList[i], Convert.ToDouble(SelectedCandleStickData.Compute("Min(" + DbTickerList[i] + "_low" + ")", "")));
                        MorningMaxDictionary.Add(DbTickerList[i], Convert.ToDouble(SelectedCandleStickData.Compute("Max(" + DbTickerList[i] + "_high" + ")", "")));
                        WorkingOrderKey.Add(DbTickerList[i], "");

                        Console.WriteLine(DbTickerList[i] + " Min: " + MorningMinDictionary[DbTickerList[i]]);
                        Console.WriteLine(DbTickerList[i] + " Max: " + MorningMaxDictionary[DbTickerList[i]]);

                    }
                }
                */

                // If necessary if condition this part with CandleDatetime>MorningRangeEndTime

                
                    DataRow PrevOscillator = StochasticOscillator.Select("TimeStamp= #" + PrevCandleDatetime.ToString() + " # ")[0];
                    DataRow CurrentOsciallator = StochasticOscillator.Select("TimeStamp= #" + CandleDatetime.ToString() + " # ")[0];

                    DataRow LastCandleStickBar = CandleData.Select("End= #" + CandleDatetime.ToString() + " # ")[0];

                    int TradeQuantity = 1;
                    string OrderTag = "ts";
                

                    foreach (KeyValuePair<InstrumentKey,Instrument> item in TTAPISubs.InstrumentDictionary)
                    {
                        Instrument Instrument_I = item.Value;
                        string DbTicker = TA.TickerConverters.ConvertFromTTAPIFields2DB(Instrument_I.Product.ToString(), Instrument_I.Name.ToString());

                        double TotalPosition = ScalperPosition.GetTotalPosition4Ticker(DbTicker);
                        double WorkingPosition = ScalperPosition.GetWorkingPosition4Ticker(DbTicker);
                        double FilledPosition = ScalperPosition.GetFilledPosition4Ticker(DbTicker);

                        //bool RangeQ = (LastCandleStickBar.Field<double>(DbTicker + "_close") < MorningMaxDictionary[DbTicker])
                        //            & (LastCandleStickBar.Field<double>(DbTicker + "_close") > MorningMinDictionary[DbTicker]);


                        if ((CurrentOsciallator.Field<double>(DbTicker + "_D1") > CurrentOsciallator.Field<double>(DbTicker + "_D2"))
                            & (PrevOscillator.Field<double>(DbTicker + "_D1") < PrevOscillator.Field<double>(DbTicker + "_D2"))
                            & (PrevOscillator.Field<double>(DbTicker + "_D1") <= 20)
                            & (TotalPosition <= 0) & (NumBets < MaxNumBets))
                        {
                            // Send New Long Order

                            NumBets++;

                            ITSLogger.Log(DbTicker + " Bullish Stochastic Crossover");
                            ITSLogger.Log("Price: " + LastCandleStickBar.Field<double>(DbTicker + "_close"));
                            ITSLogger.Log("Bid Price: " + BidPriceDictionary[DbTicker]);
                            ITSLogger.Log("D1: " + CurrentOsciallator.Field<double>(DbTicker + "_D1"));
                            ITSLogger.Log("D2: " + CurrentOsciallator.Field<double>(DbTicker + "_D2"));
                            ScalperPosition.OrderSend(DbTicker, TradeQuantity);

                            Price TradePrice = Price.FromDouble(Instrument_I.InstrumentDetails, Math.Min(LastCandleStickBar.Field<double>(DbTicker + "_close"), BidPriceDictionary[DbTicker]),Rounding.Down);
                            ITSLogger.Log("Trade Price: " + TradePrice.ToString());
                            WorkingLongEntryOrderKey[DbTicker] = ttapiUtils.Trade.SendLimitOrder(instrument: Instrument_I, price: TradePrice, qty: TradeQuantity, ttapisubs:TTAPISubs,orderTag:OrderTag);

                        }

                        if ((CurrentOsciallator.Field<double>(DbTicker + "_D1") < CurrentOsciallator.Field<double>(DbTicker + "_D2"))
                            & (PrevOscillator.Field<double>(DbTicker + "_D1") > PrevOscillator.Field<double>(DbTicker + "_D2"))
                            & (PrevOscillator.Field<double>(DbTicker + "_D1") >= 80) & (TotalPosition >= 0) & (NumBets < MaxNumBets))
                        {
                            // Send New Short Order

                            NumBets++;

                            ITSLogger.Log(DbTicker + " Bearish Stochastic Crossover");
                            ITSLogger.Log("Price: " + LastCandleStickBar.Field<double>(DbTicker + "_close"));
                            ITSLogger.Log("Ask Price: " + AskPriceDictionary[DbTicker]);
                            ITSLogger.Log("D1: " + CurrentOsciallator.Field<double>(DbTicker + "_D1"));
                            ITSLogger.Log("D2: " + CurrentOsciallator.Field<double>(DbTicker + "_D2"));
                            ScalperPosition.OrderSend(DbTicker, -TradeQuantity);

                            Price TradePrice = Price.FromDouble(Instrument_I.InstrumentDetails, Math.Max(LastCandleStickBar.Field<double>(DbTicker + "_close"), AskPriceDictionary[DbTicker]), Rounding.Up);
                            ITSLogger.Log("Trade Price: " + TradePrice.ToString());
                            WorkingShortEntryOrderKey[DbTicker] = ttapiUtils.Trade.SendLimitOrder(instrument: Instrument_I, price: TradePrice, qty: -TradeQuantity, ttapisubs: TTAPISubs, orderTag: OrderTag);

                        }

                        if ((CurrentOsciallator.Field<double>(DbTicker + "_D1") > CurrentOsciallator.Field<double>(DbTicker + "_D2"))
                            & (PrevOscillator.Field<double>(DbTicker + "_D1") <= PrevOscillator.Field<double>(DbTicker + "_D2")))
                        {
                            if (WorkingPosition < 0)
                            {
                                // Cancel entry order
                                ScalperPosition.OrderSend(DbTicker, -WorkingPosition);
                                ttapiUtils.Trade.CancelLimitOrder(orderKey: WorkingShortEntryOrderKey[DbTicker], instrument: Instrument_I, ttapisubs: TTAPISubs, logger: ITSLogger);
                                WorkingShortEntryOrderKey[DbTicker] = "";

                            }

                            if ((FilledPosition < 0)&&(WorkingPosition==0))
                            {
                                ITSLogger.Log(DbTicker + " Stop Bearish Stochastic Crossover for : " + DbTicker);
                                ITSLogger.Log("Price: " + LastCandleStickBar.Field<double>(DbTicker + "_close"));
                                ITSLogger.Log("D1: " + CurrentOsciallator.Field<double>(DbTicker + "_D1"));
                                ITSLogger.Log("D2: " + CurrentOsciallator.Field<double>(DbTicker + "_D2"));

                                // Send new order to close position
                                ScalperPosition.OrderSend(DbTicker, -FilledPosition);
                                Price TradePrice = Price.FromDouble(Instrument_I.InstrumentDetails, Math.Min(LastCandleStickBar.Field<double>(DbTicker + "_close"), BidPriceDictionary[DbTicker]), Rounding.Down);
                                WorkingShortExitOrderKey[DbTicker] = ttapiUtils.Trade.SendLimitOrder(instrument:Instrument_I, price:TradePrice, qty: -(int)FilledPosition, ttapisubs:TTAPISubs,orderTag:OrderTag);
                                

                            }
                        }

                        if ((CurrentOsciallator.Field<double>(DbTicker + "_D1") < CurrentOsciallator.Field<double>(DbTicker + "_D2"))
                            & (PrevOscillator.Field<double>(DbTicker + "_D1") >= PrevOscillator.Field<double>(DbTicker + "_D2")))
                        {
                            if (WorkingPosition > 0)
                            {
                                // Cancel entry order
                                ScalperPosition.OrderSend(DbTicker, -WorkingPosition);
                                ttapiUtils.Trade.CancelLimitOrder(orderKey: WorkingLongEntryOrderKey[DbTicker], instrument: Instrument_I, ttapisubs: TTAPISubs, logger: ITSLogger);
                                WorkingLongEntryOrderKey[DbTicker] = "";
                            }

                            if ((FilledPosition > 0)&&(WorkingPosition==0))
                            {
                                ITSLogger.Log(DbTicker + " Stop Bullish Stochastic Crossover for : " + DbTicker);
                                ITSLogger.Log("Price: " + LastCandleStickBar.Field<double>(DbTicker + "_close"));
                                ITSLogger.Log("D1: " + CurrentOsciallator.Field<double>(DbTicker + "_D1"));
                                ITSLogger.Log("D2: " + CurrentOsciallator.Field<double>(DbTicker + "_D2"));

                                // Send new order to close position
                                ScalperPosition.OrderSend(DbTicker, -FilledPosition);
                                Price TradePrice = Price.FromDouble(Instrument_I.InstrumentDetails, Math.Max(LastCandleStickBar.Field<double>(DbTicker + "_close"), AskPriceDictionary[DbTicker]), Rounding.Up);
                                WorkingLongExitOrderKey[DbTicker] = ttapiUtils.Trade.SendLimitOrder(instrument: Instrument_I, price: TradePrice, qty: -(int)FilledPosition, ttapisubs: TTAPISubs, orderTag: OrderTag);

                            }
                        }

                        
                        if ((CurrentOsciallator.Field<double>(DbTicker + "_D1") > CurrentOsciallator.Field<double>(DbTicker + "_D2"))&
                            (WorkingPosition>0)&(FilledPosition<0))
                        {
                            // Adjust Stop From Short Position
                            Price TradePrice = Price.FromDouble(Instrument_I.InstrumentDetails, Math.Min(LastCandleStickBar.Field<double>(DbTicker + "_close"), BidPriceDictionary[DbTicker]), Rounding.Down);
                            ttapiUtils.Trade.ChangeLimitOrder(orderKey: WorkingShortExitOrderKey[DbTicker], instrument: Instrument_I, ttapisubs: TTAPISubs, price: TradePrice.ToDouble(), logger: ITSLogger);

                        }

                        if ((CurrentOsciallator.Field<double>(DbTicker + "_D1") < CurrentOsciallator.Field<double>(DbTicker + "_D2"))&
                            (WorkingPosition<0)&(FilledPosition>0))
                        {
                            // Adjust Stop From Short Position

                            Price TradePrice = Price.FromDouble(Instrument_I.InstrumentDetails, Math.Max(LastCandleStickBar.Field<double>(DbTicker + "_close"), AskPriceDictionary[DbTicker]), Rounding.Up);
                            ttapiUtils.Trade.ChangeLimitOrder(orderKey: WorkingLongExitOrderKey[DbTicker], instrument: Instrument_I, ttapisubs: TTAPISubs, price: TradePrice.ToDouble(), logger: ITSLogger);

                        }


                    }
            }

            string emre = "emre";
        }

        void m_ps_FieldsUpdated(object sender, FieldsUpdatedEventArgs e)
        {
            Price BidPrice = e.Fields.GetDirectBidPriceField().Value;
            Price AskPrice = e.Fields.GetDirectAskPriceField().Value;
            double BidPriceDb;
            double AskPriceDb;
            DateTimeNow = DateTime.Now;
            TickerDB = TA.TickerConverters.ConvertFromTTAPIFields2DB(e.Fields.Instrument.Product.ToString(), e.Fields.Instrument.Name.ToString());

            if (!BidPrice.IsTradable)
            {
                return;
            }
            else
            {
                BidPriceDb = BidPrice.ToDouble();
                BidPriceDictionary[TickerDB] = BidPriceDb;
            }

            if (!AskPrice.IsTradable)
            {
                return;
            }
            else
            {
                AskPriceDb = AskPrice.ToDouble();
                AskPriceDictionary[TickerDB] = AskPriceDb;
            }

            
            candleObj.updateValues((BidPriceDb + AskPriceDb) / 2, DateTimeNow, TickerDB);

        }

        void OrderFilledLogic(object sender, OrderFilledEventArgs e)
        {
            
            Instrument Inst = ((InstrumentTradeSubscription)sender).Instrument;
            TickerDB = TA.TickerConverters.ConvertFromTTAPIFields2DB(Inst.Product.ToString(), Inst.Name.ToString());
            string TickerHead = TA.TickerheadConverters.ConvertFromTT2DB(Inst.Product.ToString());
            decimal TradeQuantity = 0;

            if (e.Fill.BuySell == BuySell.Buy)
            {
                ScalperPosition.OrderFill(TickerDB, e.Fill.Quantity);
                TradeQuantity = e.Fill.Quantity;
            }
                

            else if (e.Fill.BuySell == BuySell.Sell)
            {
                ScalperPosition.OrderFill(TickerDB, -e.Fill.Quantity);
                TradeQuantity = -e.Fill.Quantity;
            }
                

            if (e.FillType == FillType.Full)
            {
                Console.WriteLine("Order was fully filled for {0} at {1}.", e.Fill.Quantity, e.Fill.MatchPrice);

                if (e.Fill.SiteOrderKey == WorkingLongEntryOrderKey[TickerDB])
                {
                    WorkingLongEntryOrderKey[TickerDB] = "";
                }

                else if (e.Fill.SiteOrderKey == WorkingLongExitOrderKey[TickerDB])
                {
                    WorkingLongExitOrderKey[TickerDB] = "";
                }

                else if (e.Fill.SiteOrderKey == WorkingShortEntryOrderKey[TickerDB])
                {
                    WorkingShortEntryOrderKey[TickerDB] = "";
                }

                else if (e.Fill.SiteOrderKey == WorkingShortExitOrderKey[TickerDB])
                {
                    WorkingShortExitOrderKey[TickerDB] = "";
                }
               
            }
            else
            {
                Console.WriteLine("Order was partially filled for {0} at {1}.", e.Fill.Quantity, e.Fill.MatchPrice);
            }

            TA.Strategy.LoadTrade2Strategy(ticker: TickerDB, trade_price: (decimal)TA.PriceConverters.FromTT2DB(ttPrice: Convert.ToDecimal(e.Fill.MatchPrice.ToString()),
                tickerHead: TickerHead), trade_quantity: TradeQuantity, instrument: "F", alias: "TS_Jan_2017", conn: conn);

            //Console.WriteLine("Average Buy Price = {0} : Net Position = {1} : P&L = {2}", m_ts.ProfitLossStatistics.BuyAveragePrice,
            //   m_ts.ProfitLossStatistics.NetPosition, m_ts.ProfitLoss.AsPrimaryCurrency);
        }

        public void Dispose()
        {
            TTAPISubs.Dispose();
        }
    }
}
