using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using CalendarUtilities;
using ttapiUtils;

namespace SaveVolume
{
    using TradingTechnologies.TTAPI;

    class saveVolume: IDisposable

    {
        /// <summary>
        /// Declare the TTAPI objects
        /// </summary>
        /// 
        public ttapiUtils.Subscription TTAPISubs;

        public List<string> InstrumentList = new List<string>();
        
        public Dictionary<InstrumentKey, InstrumentLookupSubscription> IlsDictionary;
        List<EventHandler<InstrumentLookupSubscriptionEventArgs>> ilsUpdateList;


        private string m_username;
        private string m_password;
        private string OutputFolder;
        private TextWriter sw;


        public List<string> TickerheadList { get; set; }


        /// <summary>
        /// Private default constructor
        /// </summary>
        private saveVolume() 
        { 
        }

        /// <summary>
        /// Primary constructor
        /// </summary>
        public saveVolume(string u, string p)
        {
            m_username = u;
            m_password = p;
   
            OutputFolder = TA.DirectoryNames.GetDirectoryName(ext: "ttapiContractVolume") + 
                TA.DirectoryNames.GetDirectoryExtension(DateTime.Now.Date);
            System.IO.Directory.CreateDirectory(OutputFolder);

            ilsUpdateList = new List<EventHandler<InstrumentLookupSubscriptionEventArgs>>();

            sw = new StreamWriter(OutputFolder + "/ContractList.csv");

            sw.WriteLine("{0},{1},{2},{3},{4}", "InstrumentName",
                        "MarketKey",
                        "ProductType",
                        "ProductName",
                        "Volume");

            sw.Flush();

            TTAPISubs = new ttapiUtils.Subscription(m_username, m_password);

            IlsDictionary = TTAPISubs.IlsDictionary;
            
            TTAPISubs.asu_update = TTAPISubs.startProductLookupSubscriptions;
            TTAPISubs.PLSEventHandler = TTAPISubs.Subscribe2InstrumentCatalogs;
            TTAPISubs.ICUEventHandler = TTAPISubs.StartInstrumentLookupSubscriptionsFromCatalog;

            ilsUpdateList.Add(TTAPISubs.startPriceSubscriptions);
            TTAPISubs.priceUpdatedEventHandler = WriteVolume2File;

            TTAPISubs.ilsUpdateList = ilsUpdateList;

        }

        void WriteVolume2File(object sender, FieldsUpdatedEventArgs e)
        {
            if (e.Error == null)
            {
                if (e.UpdateType == UpdateType.Snapshot)
                {
                    // Received a market data snapshot
                    Console.WriteLine(DateTime.Now.ToString("h:mm:ss tt"));
                    
                    sw.WriteLine("{0},{1},{2},{3},{4}", e.Fields.Instrument.Name, 
                        e.Fields.Instrument.Key.MarketKey.ToString(),
                        e.Fields.Instrument.Product.Type.ToString(),
                        e.Fields.Instrument.Product.Name.ToString(),
                        e.Fields[FieldId.TotalTradedQuantity].FormattedValue);

                    sw.Flush();

                    bool IlsDictionaryCompleteQ = TTAPISubs.IlsDictionaryCompleteQ;

                    for (int i = 0; i < ilsUpdateList.Count; i++)
                    {
                        IlsDictionary[e.Fields.Instrument.Key].Update -= ilsUpdateList[i];
                    }

                    
                    IlsDictionary[e.Fields.Instrument.Key].Dispose();
                    IlsDictionary[e.Fields.Instrument.Key] = null;
                    IlsDictionary.Remove(e.Fields.Instrument.Key);

                    if ((IlsDictionary.Count == 0)&IlsDictionaryCompleteQ)
                    {
                        Dispatcher.Current.BeginInvoke(new Action(Dispose));
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

        public void Dispose()
        {
            TTAPISubs.Dispose();
        }

    }
    }
