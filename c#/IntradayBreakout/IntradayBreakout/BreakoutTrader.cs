using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using CalendarUtilities;
using ContractUtilities;
using TA;
using System.Data;
using ttapiUtils;


namespace IntradayBreakout
{
    using TradingTechnologies.TTAPI;
    using System.Reflection;
    

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
        List<TA.ContractVolume> ttapiTickerList;
        List<string> dbTickerList;
        ContractUtilities.ContractList liquidContractList;
        DateTime todayDate;
        DateTime dateTimeNow;
        DateTime startTime;
        DateTime endTime;

        string[] instrumentList = new string[] { "ES", "EC", "CL" };
        List<double> rangeMinList;
        List<double> rangeMaxList;
        List<int> positionList;
        Double bidPrice;
        Double askPrice;
        Double midPrice;
        string tickerDB;

        

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
            OutputFolder = TA.DirectoryNames.overnightCandlestickDirectory + BusinessDays.GetDirectoryExtension(todayDate);

            instrumentList = ContractUtilities.ContractMetaInfo.cmeFuturesTickerheadList.Union(ContractUtilities.ContractMetaInfo.iceFuturesTickerheadList).ToArray();

            liquidContractList = new ContractUtilities.ContractList(instrumentList);
            ttapiTickerList = liquidContractList.ttapiTickerList;
            dbTickerList = liquidContractList.dbTickerList;

            candleStickData = new DataTable();
            rangeMinList = new List<double>();
            rangeMaxList = new List<double>();
            positionList = new List<int>();
            candleStickData.ReadXml(OutputFolder + "/" + TA.FileNames.candlestick_signal_file);

            startTime = new DateTime(todayDate.Year, todayDate.Month, todayDate.Day, 8, 30, 0);
            endTime = new DateTime(todayDate.Year, todayDate.Month, todayDate.Day, 9, 0, 0);    //900

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
                positionList.Add(0);

            }

            ttapiSubs = new ttapiUtils.Subscription(m_username, m_password);
            ttapiSubs.ttapiTickerList = ttapiTickerList;
            ttapiSubs.ils_update = ttapiSubs.startPriceSubscriptions;
            ttapiSubs.asu_update = ttapiSubs.startInstrumentLookupSubscriptions;
            ttapiSubs.priceUpdatedEventHandler = m_ps_FieldsUpdated;

        }


        /// <summary>
        /// Event notification for price update
        /// </summary>
        void m_ps_FieldsUpdated(object sender, FieldsUpdatedEventArgs e)
        {
            if (e.Error == null)
            {
                dateTimeNow = DateTime.Now;

                bidPrice = Convert.ToDouble(e.Fields.GetDirectBidPriceField().FormattedValue);
                askPrice = Convert.ToDouble(e.Fields.GetDirectAskPriceField().FormattedValue);

                midPrice = (bidPrice + askPrice) / 2;
                tickerDB = TA.TickerConverters.ConvertFromTTAPIFields2DB(e.Fields.Instrument.Product.ToString(), e.Fields.Instrument.Name.ToString());

                int tickerIndex = Enumerable.Range(0, dbTickerList.Count).Where(i => dbTickerList[i] == tickerDB).ToList()[0];

                if ((midPrice<rangeMinList[tickerIndex]) && (positionList[tickerIndex] == 0))
                {
                    Console.WriteLine("Bearish Breakout in " + tickerDB);
                    Console.WriteLine("Range Min: " + rangeMinList[tickerIndex]);
                    Console.WriteLine("Current Price: " + midPrice);
                    positionList[tickerIndex] = -1;
                }
                else if ((midPrice > rangeMaxList[tickerIndex]) && (positionList[tickerIndex] == 0))
                {
                    Console.WriteLine("Bullish Breakout in " + tickerDB);
                    Console.WriteLine("Range Max: " + rangeMaxList[tickerIndex]);
                    Console.WriteLine("Current Price: " + midPrice);
                    positionList[tickerIndex] = 1;
                }

                else if (positionList[tickerIndex] == 1)
                {
                    Console.WriteLine("Pnl in " + tickerDB + ": " + (midPrice-rangeMaxList[tickerIndex]).ToString());

                }

                else if (positionList[tickerIndex] == -1)
                {
                    Console.WriteLine("Pnl in " + tickerDB + ": " + (rangeMinList[tickerIndex]-midPrice).ToString());

                }

            }
            else
            {
                if (e.Error.IsRecoverableError == false)
                {
                    Console.WriteLine("Unrecoverable price subscription error: {0}", e.Error.Message);
                    ttapiSubs.Dispose1();
                }
            }
        }

        public void Dispose()
        {
            ttapiSubs.Dispose1();
        }

        
       
    }
}
