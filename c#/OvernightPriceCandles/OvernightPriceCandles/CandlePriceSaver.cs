using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using TA;
using IOUtilities;
using System.Data;
using DataAnalysis;
using CalendarUtilities;
using ContractUtilities;
using ttapiUtils;

namespace OvernightPriceCandles
{
    using TradingTechnologies.TTAPI;
    using System.Reflection;
    
    class CandlePriceSaver : IDisposable
    {
        /// <summary>
        /// Declare the TTAPI objects
        /// </summary>
        public ttapiUtils.Subscription ttapiSubs;
        
        private string m_username = "";
        private string m_password = "";
        private string OutputFolder;
        
        ContractUtilities.ContractList liquidContractList;
        string[] instrumentList;
        DataTable candlestickData;
        CandleStick candleObj;
        Double bidPrice;
        Double askPrice;
        Double midPrice;
        
        string tickerDB;
        DateTime dateTimeNow;
        DateTime startTime;
        DateTime endTime;

        //StringBuilder sb = new StringBuilder();

        /// <summary>
        /// Private default constructor
        /// </summary>
        private CandlePriceSaver() 
        { 
        }

        /// <summary>
        /// Primary constructor
        /// </summary>
        public CandlePriceSaver(string u, string p)
        {
            m_username = u;
            m_password = p;

            instrumentList = ContractUtilities.ContractMetaInfo.cmeFuturesTickerheadList.Union(ContractUtilities.ContractMetaInfo.iceFuturesTickerheadList).ToArray();

            liquidContractList = new ContractUtilities.ContractList(instrumentList);
            
            DateTime referanceDate = DateTime.Today;

            int minInterval = 10;   //30

            startTime = new DateTime(referanceDate.Year, referanceDate.Month, referanceDate.Day, 1, 0, 0);
            endTime = new DateTime(referanceDate.Year, referanceDate.Month, referanceDate.Day, 9, 0, 0);    //900

            candleObj = new DataAnalysis.CandleStick(startTime, endTime, liquidContractList.dbTickerList.ToArray(), minInterval);

            OutputFolder = TA.DirectoryNames.GetDirectoryName(ext: "overnightCandlestick") + 
                TA.DirectoryNames.GetDirectoryExtension(DateTime.Now.Date);
            System.IO.Directory.CreateDirectory(OutputFolder);

            ttapiSubs = new ttapiUtils.Subscription(m_username, m_password);
            ttapiSubs.dbTickerList = liquidContractList.dbTickerList;
            ttapiSubs.ilsUpdateList = new List<EventHandler<InstrumentLookupSubscriptionEventArgs>> { ttapiSubs.startPriceSubscriptions };
            ttapiSubs.asu_update = ttapiSubs.startInstrumentLookupSubscriptions;
            ttapiSubs.priceUpdatedEventHandler = m_ps_FieldsUpdated;

        }


        /// <summary>
        /// Event notification for price update
        /// </summary>
        void m_ps_FieldsUpdated(object sender, FieldsUpdatedEventArgs e)
        {
            dateTimeNow = DateTime.Now;

            if (dateTimeNow>endTime)
            {
                candlestickData = candleObj.data;
                candlestickData.TableName = "candleStick";
                candlestickData.WriteXml(OutputFolder + "/" + TA.FileNames.candlestick_signal_file, XmlWriteMode.WriteSchema);
                Dispatcher.Current.BeginInvoke(new Action(Dispose));
                return;
            }


            if (e.Error == null)
            {

                if (string.IsNullOrEmpty(e.Fields.GetDirectBidPriceField().FormattedValue))
                { return; }
                else
                { bidPrice = Convert.ToDouble(e.Fields.GetDirectBidPriceField().FormattedValue); }

                if (string.IsNullOrEmpty(e.Fields.GetDirectAskPriceField().FormattedValue))
                { return; }
                else
                { askPrice = Convert.ToDouble(e.Fields.GetDirectAskPriceField().FormattedValue); }

                midPrice = (bidPrice + askPrice) / 2;
                tickerDB = TA.TickerConverters.ConvertFromTTAPIFields2DB(e.Fields.Instrument.Product.ToString(), e.Fields.Instrument.Name.ToString());

                candleObj.updateValues((bidPrice + askPrice) / 2, dateTimeNow, tickerDB);
            }     
        }

        /// <summary>
        /// Shuts down the TT API
        /// </summary>
        public void Dispose()
        {
            ttapiSubs.Dispose();

        }

       
    }
}
