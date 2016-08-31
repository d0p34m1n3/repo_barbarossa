using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using CalendarUtilities;
using ContractUtilities;
using Portfolio;
using TA;
using System.Data;
using ttapiUtils;
using Shared;

namespace IntradayBreakout
{
    using TradingTechnologies.TTAPI;
    using TradingTechnologies.TTAPI.Tradebook;
    using System.Reflection;
    using System.Threading;
    

    class BreakoutTrader : IDisposable
    {
        /// <summary>
        /// Declare the TTAPI objects
        /// </summary>
        
        
        public ttapiUtils.Subscription ttapiSubs;
        private string m_username = "";
        private string m_password = "";
        private string OutputFolder;
        
        DataTable candleStickData;
        DataRow[] selectedCandleStickData;
        List<TA.ttapiTicker> ttapiTickerList;
        List<string> dbTickerList;
        ContractUtilities.ContractList liquidContractList;
        DateTime todayDate;
        DateTime dateTimeNow;
        DateTime startTime;
        DateTime endTime;
        DateTime LastTradeEntryTime;
        DateTime DateTimePastPnlDisplay;

        string[] instrumentList = new string[] { "ES", "EC", "CL" };
        List<double> rangeMinList;
        List<double> rangeMaxList;
        bool LiveTradingQ = true;
        double bidPrice;
        double askPrice;
        double midPrice;
        double spreadPrice;
        double FilledPosition;
        double WorkingPosition;
        double TotalPosition;
        double PortfolioStd;
        double PortfolioVarChange;
        double PortfolioStdAfter;
        double StdPerBet = 200;  //200
        double PortfolioStdLimit = 200; //200
        double Std4Ticker;
        int Qty4Ticker;
        int MaxQty4Ticker = 1;
        int MaxOrdersSent = 1;
        int TotalOrders;
        Dictionary<string, double> StdDict;
        Dictionary<string, int> QtyDict;

        string tickerDB;
        Portfolio.Position BreakoutPosition;
        DataTable CovMatrix;


        List<EventHandler<InstrumentLookupSubscriptionEventArgs>> ilsUpdateList;
        private InstrumentTradeSubscription ts;
        public Dictionary<InstrumentKey, InstrumentTradeSubscription> TsDictionary;
        StreamWriter LogFile;
        Logger BreakoutLogger;

        /// <summary>
        /// Private default constructor
        /// </summary>
        private BreakoutTrader() 
        { 
        }

        /// <summary>
        /// Primary constructor
        /// </summary>
        public BreakoutTrader(string u, string p)
        {
            m_username = u;
            m_password = p;

            todayDate = DateTime.Now.Date;
            OutputFolder = TA.DirectoryNames.GetDirectoryName("overnightCandlestick") + TA.DirectoryNames.GetDirectoryExtension(todayDate);

            LogFile = new StreamWriter(OutputFolder + "/Log" + DateTime.Now.ToString("HHmmss") + ".txt", true);
            BreakoutLogger = new Logger(LogFile);

            instrumentList = ContractUtilities.ContractMetaInfo.cmeFuturesTickerheadList.Union(ContractUtilities.ContractMetaInfo.iceFuturesTickerheadList).ToArray();

            liquidContractList = new ContractUtilities.ContractList(instrumentList);
            ttapiTickerList = liquidContractList.ttapiTickerList;
            dbTickerList = liquidContractList.dbTickerList;
            DateTimePastPnlDisplay = DateTime.MinValue;


            BreakoutPosition = new Portfolio.Position(fullTickerList: dbTickerList);

            CovMatrix = Risk.PorfolioRisk.LoadCovMatrix();

            candleStickData = new DataTable();

            rangeMinList = new List<double>();
            rangeMaxList = new List<double>();
            ilsUpdateList = new List<EventHandler<InstrumentLookupSubscriptionEventArgs>>();
            candleStickData.ReadXml(OutputFolder + "/" + TA.FileNames.candlestick_signal_file);

            StdDict = new Dictionary<string, double>();
            QtyDict = new Dictionary<string, int>();


            startTime = new DateTime(todayDate.Year, todayDate.Month, todayDate.Day, 8, 30, 0);
            endTime = new DateTime(todayDate.Year, todayDate.Month, todayDate.Day, 9, 0, 0);    //900
            LastTradeEntryTime = new DateTime(todayDate.Year, todayDate.Month, todayDate.Day, 9, 20, 0);    //920

            selectedCandleStickData = candleStickData.Select("start>= #" + startTime.ToString() + "# AND end<= #" + endTime.ToString() + " # ");

            for (int i = 0; i < dbTickerList.Count; i++)
            {
                double rangeMin = double.MaxValue;
                double rangeMax = double.MinValue;


                for (int j = 0; j < selectedCandleStickData.Length; j++)
                {
                    rangeMax = Math.Max(selectedCandleStickData[j].Field<Double>(dbTickerList[i] + "_high"), rangeMax);
                    rangeMin = Math.Min(selectedCandleStickData[j].Field<Double>(dbTickerList[i] + "_low"), rangeMin);
                }
                rangeMinList.Add(rangeMin);
                rangeMaxList.Add(rangeMax);
                
                StdDict.Add(dbTickerList[i], Risk.PorfolioRisk.GetStd4Ticker(dbTickerList[i], CovMatrix));
                QtyDict.Add(dbTickerList[i], (int)Math.Min(MaxQty4Ticker, Math.Floor(StdPerBet / StdDict[dbTickerList[i]])));
                
            }

            //

            ttapiSubs = new ttapiUtils.Subscription(m_username, m_password);
            ttapiSubs.dbTickerList = dbTickerList;

            ilsUpdateList.Add(ttapiSubs.startPriceSubscriptions);
            ilsUpdateList.Add(ttapiSubs.startTradeSubscriptions);

            ttapiSubs.ilsUpdateList = ilsUpdateList;
            ttapiSubs.asu_update = ttapiSubs.startInstrumentLookupSubscriptions;
            ttapiSubs.priceUpdatedEventHandler = BreakoutAlgo;
            ttapiSubs.orderFilledEventHandler = BreakoutStopLogic;

        }

        /// <summary>
        /// Event notification for price update
        /// </summary>

        void BreakoutAlgo(object sender, FieldsUpdatedEventArgs e)
        {
            if (e.Error == null)
            {
                dateTimeNow = DateTime.Now;

                if ((dateTimeNow-DateTimePastPnlDisplay).Minutes>10)
                {
                    DisplayPnl();
                    DateTimePastPnlDisplay = dateTimeNow;
                }

                if (string.IsNullOrEmpty(e.Fields.GetDirectBidPriceField().FormattedValue))
                { return; }
                else
                { bidPrice = Convert.ToDouble(e.Fields.GetDirectBidPriceField().FormattedValue); }

                if (string.IsNullOrEmpty(e.Fields.GetDirectAskPriceField().FormattedValue))
                { return; }
                else
                { askPrice = Convert.ToDouble(e.Fields.GetDirectAskPriceField().FormattedValue); }

                midPrice = (bidPrice + askPrice) / 2;
                spreadPrice = askPrice - bidPrice;
                tickerDB = TA.TickerConverters.ConvertFromTTAPIFields2DB(e.Fields.Instrument.Product.ToString(), e.Fields.Instrument.Name.ToString());

                string TickerHead = ContractUtilities.ContractMetaInfo.GetContractSpecs(tickerDB).tickerHead;
                string ExchangeName = ContractUtilities.ContractMetaInfo.GetExchange4Tickerhead(TickerHead);

                FilledPosition = BreakoutPosition.GetFilledPosition4Ticker(tickerDB);
                WorkingPosition = BreakoutPosition.GetWorkingPosition4Ticker(tickerDB);
                TotalPosition = FilledPosition + WorkingPosition;
                Std4Ticker = StdDict[tickerDB];
                Qty4Ticker = QtyDict[tickerDB];
                TotalOrders = BreakoutPosition.PositionWithAllOrders.Select("Qty<>0").Length;

                int tickerIndex = Enumerable.Range(0, dbTickerList.Count).Where(i => dbTickerList[i] == tickerDB).ToList()[0];
                ts = ttapiSubs.TsDictionary[e.Fields.Instrument.Key];

                if ((midPrice < rangeMinList[tickerIndex]) &&
                    (FilledPosition == 0) && (WorkingPosition==0) &&
                    (dateTimeNow < LastTradeEntryTime))
                {
                    if (Std4Ticker>StdPerBet)
                    {
                        BreakoutLogger.Log(tickerDB + " has std " + Std4Ticker + " too large for current risk limits! ");
                        return;
                    }

                    PortfolioVarChange = Risk.PorfolioRisk.GetChangeInRiskAfterTickerInclusion(BreakoutPosition.PositionWithAllOrders, CovMatrix,
                        tickerDB, -Qty4Ticker);

                    PortfolioStdAfter = Math.Sqrt(Math.Pow(PortfolioStd, 2) + PortfolioVarChange);

                    if (double.IsNaN(PortfolioStdAfter))
                    {
                        BreakoutLogger.Log("After trading " + tickerDB + " portfolio std would be: " + PortfolioStdAfter);
                        BreakoutLogger.Log("PortfolioStd: " + PortfolioStd);
                        BreakoutLogger.Log("PortfolioVarChange: " + PortfolioVarChange);
                        
                        return;
                    }

                    if (PortfolioStdAfter > PortfolioStdLimit)
                    {
                        BreakoutLogger.Log("After trading " + tickerDB + " portfolio std would be: " + PortfolioStdAfter + " too large!");
                        return;
                    }
                    else
                    {
                        BreakoutLogger.Log(tickerDB + " trade satisfies risk limit with target portfolio risk: " + PortfolioStdAfter);
                    }

                    BreakoutLogger.Log("Bearish Breakout in " + tickerDB);
                    BreakoutLogger.Log("Range Min: " + rangeMinList[tickerIndex]);
                    BreakoutLogger.Log("Current Price: " + midPrice);
                    BreakoutLogger.Log("Std 4 Ticker: " + Std4Ticker);
                    
                    if (TotalOrders>=MaxOrdersSent)
                    {
                        BreakoutLogger.Log("Total orders sent : " + TotalOrders + " is not smaller than " +MaxOrdersSent);
                        return;
                    }

                    if (LiveTradingQ)
                    {
                        ttapiUtils.Trade.SendLimitOrder(e.Fields.Instrument, e.Fields.GetBestAskPriceField().Value, -Qty4Ticker, ttapiSubs);
                        BreakoutPosition.OrderSend(tickerDB, -Qty4Ticker); 
                    }
                       
                    else
                    {
                        BreakoutPosition.OrderSend(tickerDB, -Qty4Ticker);
                        BreakoutPosition.OrderFill(tickerDB, -Qty4Ticker);
                    }
                    PortfolioStd = Risk.PorfolioRisk.GetPortfolioRiskFromCovMatrix(BreakoutPosition.PositionWithAllOrders, CovMatrix);
                    BreakoutLogger.Log("Portfolio Std After Fill: " + PortfolioStd);
                }
                else if ((midPrice > rangeMaxList[tickerIndex]) &&
                    (FilledPosition == 0) && (WorkingPosition == 0)&&
                    (dateTimeNow < LastTradeEntryTime))
                {
                    if (Std4Ticker > StdPerBet)
                    {
                        BreakoutLogger.Log(tickerDB + " has std " + Std4Ticker + " too large for current risk limits! ");
                        return;
                    }

                    PortfolioVarChange = Risk.PorfolioRisk.GetChangeInRiskAfterTickerInclusion(BreakoutPosition.PositionWithAllOrders, CovMatrix,
                        tickerDB, Qty4Ticker);

                    PortfolioStdAfter = Math.Sqrt(Math.Pow(PortfolioStd, 2) + PortfolioVarChange);
                 
                    if (double.IsNaN(PortfolioStdAfter))
                    {
                        BreakoutLogger.Log("After trading " + tickerDB + " portfolio std would be: " + PortfolioStdAfter);
                        BreakoutLogger.Log("PortfolioStd: " + PortfolioStd);
                        BreakoutLogger.Log("PortfolioVarChange: " + PortfolioVarChange);

                        return;
                    }

                    if (PortfolioStdAfter > PortfolioStdLimit)
                    {
                        BreakoutLogger.Log("After trading " + tickerDB + " portfolio std would be: " + PortfolioStdAfter + " too large!");
                        return;
                    }
                    else
                    {
                        BreakoutLogger.Log(tickerDB + " trade satisfies risk limit with target portfolio risk: " + PortfolioStdAfter);
                    }

                    BreakoutLogger.Log("Bullish Breakout in " + tickerDB);
                    BreakoutLogger.Log("Range Max: " + rangeMaxList[tickerIndex]);
                    BreakoutLogger.Log("Current Price: " + midPrice);
                    BreakoutLogger.Log("Std 4 Ticker: " + Std4Ticker);

                    if (TotalOrders >= MaxOrdersSent)
                    {
                        BreakoutLogger.Log("Total orders sent : " + TotalOrders + " is not smaller than " + MaxOrdersSent);
                        return;
                    }
                    
                    if (LiveTradingQ)
                    {
                        ttapiUtils.Trade.SendLimitOrder(e.Fields.Instrument, e.Fields.GetBestAskPriceField().Value, Qty4Ticker, ttapiSubs);
                        BreakoutPosition.OrderSend(tickerDB, Qty4Ticker);
                    }
                    else
                    {
                        BreakoutPosition.OrderSend(tickerDB, Qty4Ticker);
                        BreakoutPosition.OrderFill(tickerDB, Qty4Ticker);
                    }
                    PortfolioStd = Risk.PorfolioRisk.GetPortfolioRiskFromCovMatrix(BreakoutPosition.PositionWithAllOrders, CovMatrix);
                    BreakoutLogger.Log("Portfolio Std After Fill: " + PortfolioStd);
                    
                }

                else if ((FilledPosition > 0) && (WorkingPosition == 0) && (midPrice < rangeMinList[tickerIndex]))
                {
                    BreakoutLogger.Log("Pnl in " + tickerDB + ": " + (midPrice - rangeMaxList[tickerIndex]).ToString());

                    if (LiveTradingQ)
                    {
                        ttapiUtils.Trade.SendLimitOrder(e.Fields.Instrument, e.Fields.GetBestAskPriceField().Value, -(int)FilledPosition, ttapiSubs);
                        BreakoutPosition.OrderSend(tickerDB, -FilledPosition);
                    }

                    else
                    {
                        BreakoutPosition.OrderSend(tickerDB, -FilledPosition);
                        BreakoutPosition.OrderFill(tickerDB, -FilledPosition);
                       
                    }
                    PortfolioStd = Risk.PorfolioRisk.GetPortfolioRiskFromCovMatrix(BreakoutPosition.PositionWithAllOrders, CovMatrix);
                    BreakoutLogger.Log("Portfolio Std After Fill: " + PortfolioStd);

                }

                else if ((FilledPosition < 0) && (WorkingPosition == 0) && (midPrice > rangeMaxList[tickerIndex]))
                {
                    BreakoutLogger.Log("Pnl in " + tickerDB + ": " + (rangeMinList[tickerIndex] - midPrice).ToString());

                    if (LiveTradingQ)
                    {
                        ttapiUtils.Trade.SendLimitOrder(e.Fields.Instrument, e.Fields.GetBestAskPriceField().Value, -(int)FilledPosition, ttapiSubs);
                        BreakoutPosition.OrderSend(tickerDB, -FilledPosition);
                       
                    }
                    else
                    {
                        BreakoutPosition.OrderSend(tickerDB, -FilledPosition);
                        BreakoutPosition.OrderFill(tickerDB, -FilledPosition);
                    }
                    PortfolioStd = Risk.PorfolioRisk.GetPortfolioRiskFromCovMatrix(BreakoutPosition.PositionWithAllOrders, CovMatrix);
                    BreakoutLogger.Log("Portfolio Std After Fill: " + PortfolioStd);
                }
            }
            else
            {
                if (e.Error.IsRecoverableError == false)
                {
                    Console.WriteLine("Unrecoverable price subscription error: {0}", e.Error.Message);
                    ttapiSubs.Dispose();
                }
            }
        }

        void DisplayPnl()
        {
            Console.WriteLine("P&L REPORT:");
            TsDictionary = ttapiSubs.TsDictionary;
            foreach (InstrumentKey IKey in TsDictionary.Keys.ToList())
            {
                 tickerDB = TA.TickerConverters.ConvertFromTTAPIFields2DB(TsDictionary[IKey].Instrument.Product.ToString(), 
                     TsDictionary[IKey].Instrument.Name.ToString());

                FilledPosition = BreakoutPosition.GetFilledPosition4Ticker(tickerDB);
                WorkingPosition = BreakoutPosition.GetWorkingPosition4Ticker(tickerDB);
                TotalPosition = FilledPosition + WorkingPosition;

                if (TotalPosition!=0)
                {
                    Console.WriteLine(tickerDB + "P&L: " + TsDictionary[IKey].ProfitLoss.AsPrimaryCurrency);
                }
            }
        }

        void BreakoutStopLogic(object sender, OrderFilledEventArgs e)
        {

            Instrument inst = ((InstrumentTradeSubscription)sender).Instrument;
            tickerDB = TA.TickerConverters.ConvertFromTTAPIFields2DB(inst.Product.ToString(), inst.Name.ToString());

            if (e.Fill.BuySell==BuySell.Buy)
                BreakoutPosition.OrderFill(tickerDB, e.Fill.Quantity);
              
            else if (e.Fill.BuySell == BuySell.Sell)
                BreakoutPosition.OrderFill(tickerDB, -e.Fill.Quantity);

            if (e.FillType == FillType.Full)
            {
                Console.WriteLine("Order was fully filled for {0} at {1}.", e.Fill.Quantity, e.Fill.MatchPrice);
            }
            else
            {
                Console.WriteLine("Order was partially filled for {0} at {1}.", e.Fill.Quantity, e.Fill.MatchPrice);
            }

            //Console.WriteLine("Average Buy Price = {0} : Net Position = {1} : P&L = {2}", m_ts.ProfitLossStatistics.BuyAveragePrice,
            //   m_ts.ProfitLossStatistics.NetPosition, m_ts.ProfitLoss.AsPrimaryCurrency);
        }

        public void Dispose()
        {
            ttapiSubs.Dispose();
        }
 
    }
}
