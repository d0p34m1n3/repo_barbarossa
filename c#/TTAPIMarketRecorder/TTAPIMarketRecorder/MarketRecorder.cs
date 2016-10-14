using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using CalendarUtilities;
using IOUtilities;
using System.Data;
using TA;



namespace TTAPIMarketRecorder
{
    using TradingTechnologies.TTAPI;
    using System.Reflection;
    using CsvHelper;
    using CsvHelper.Configuration;

    /// <summary>
    /// Main TT API class
    /// </summary>
    /// 

    class MarketRecorder : IDisposable
    {
        /// <summary>
        /// Declare the TTAPI objects
        /// </summary>
        
        private string m_username = "";
        private string m_password = "";
        private string OutputFolder;
        IEnumerable<ContractVolume> ContractVolumeList;
        List<ttapiTicker> FilteredList;
        Dictionary<string, StreamWriter> PriceFileDictionary;
        public ttapiUtils.Subscription TTAPISubs;
        DateTime EndTime;

        /// <summary>
        /// Private default constructor
        /// </summary>
        private MarketRecorder() 
        { 
        }

        /// <summary>
        /// Primary constructor
        /// </summary>
        public MarketRecorder(string u, string p)
        {
            m_username = u;
            m_password = p;

            OutputFolder = TA.DirectoryNames.GetDirectoryName(ext: "ttapiBidAsk") + TA.DirectoryNames.GetDirectoryExtension(DateTime.Now.Date);

            System.IO.Directory.CreateDirectory(OutputFolder);

            PriceFileDictionary = new Dictionary<string, StreamWriter>();
            FilteredList = new List<ttapiTicker>();

            DateTime referanceDate = DateTime.Today;
            EndTime = new DateTime(referanceDate.Year, referanceDate.Month, referanceDate.Day, 15, 45, 0); 


            ContractVolumeList = TA.LoadContractVolumeFile.GetContractVolumes(folderDate: BusinessDays.GetBusinessDayShifted(-1));
            FilteredList = ContractVolumeList.Where(x => x.Volume > 100).ToList().OfType<ttapiTicker>().ToList();

                for (int i = 0; i < FilteredList.Count;  i++)
                {
                    //Console.WriteLine(FilteredList[i].instrumentName);
                    PriceFileDictionary.Add(FilteredList[i].instrumentName, new StreamWriter(OutputFolder + "/" + FilteredList[i].instrumentName.Replace("/", "-").Replace(":", "-") + ".csv"));
                }

                TTAPISubs = new ttapiUtils.Subscription(m_username, m_password);
                TTAPISubs.TTAPITickerList = FilteredList;
                TTAPISubs.ilsUpdateList = new List<EventHandler<InstrumentLookupSubscriptionEventArgs>> { TTAPISubs.startPriceSubscriptions };
                TTAPISubs.asu_update = TTAPISubs.startInstrumentLookupSubscriptionsFromTTAPITickers;
                TTAPISubs.priceUpdatedEventHandler = m_ps_FieldsUpdated;

        }

        /// <summary>
        /// Event notification for price update
        /// </summary>
        void m_ps_FieldsUpdated(object sender, FieldsUpdatedEventArgs e)
        {

            if (DateTime.Now > EndTime)
            {
                Dispatcher.Current.BeginInvoke(new Action(Dispose));
                return;
            }

            if (e.Error == null)
            {
                if (e.UpdateType == UpdateType.Snapshot)
                {
                    // Received a market data snapshot
                    //Console.WriteLine("Market Data Snapshot:");

                    foreach (FieldId id in e.Fields.GetFieldIds())
                    {

                        //e.Fields.Instrument

                        PriceFileDictionary[e.Fields.Instrument.Name.ToString()].AutoFlush = true;
                        PriceFileDictionary[e.Fields.Instrument.Name.ToString()].WriteLine("{0},{1},{2}", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"), id.ToString(), e.Fields[id].FormattedValue);
                        
                    }
                }
                else
                {
                    // Only some fields have changed
                    //Console.WriteLine("Market Data Update:");

                    foreach (FieldId id in e.Fields.GetChangedFieldIds())
                    {
                        Console.WriteLine("    {0} : {1}", id.ToString(), e.Fields[id].FormattedValue);

                        PriceFileDictionary[e.Fields.Instrument.Name.ToString()].WriteLine("{0},{1},{2}", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"),id.ToString(), e.Fields[id].FormattedValue);
                        
                    }
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

        /// <summary>
        /// Shuts down the TT API
        /// </summary>
        public void Dispose()
        {
            TTAPISubs.Dispose();
        }

    }
}
