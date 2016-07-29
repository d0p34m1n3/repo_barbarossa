using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using CalendarUtilities;

namespace SaveVolume
{
    using TradingTechnologies.TTAPI;

    class saveVolume: IDisposable

    {
        /// <summary>
        /// Declare the TTAPI objects
        /// </summary>
        private UniversalLoginTTAPI m_apiInstance = null;
        private WorkerDispatcher m_disp = null;
        private bool m_disposed = false;
        private object m_lock = new object();
        private List<ProductLookupSubscription> plsList = null;
        private InstrumentCatalogSubscription ics = null;
        public List<string> InstrumentList = new List<string>();
        private InstrumentLookupSubscription m_req = null;
        private PriceSubscription m_ps = null;

        private string m_username = "ekocatulum";
        private string m_password = "pompei1789";
        private string OutputFolder;
        private TextWriter sw;
        MarketKey mkey;

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
            OutputFolder = "C:/Research/data/intraday_data/tt_api/" + BusinessDays.GetDirectoryExtension(DateTime.Now.Date);
            System.IO.Directory.CreateDirectory(OutputFolder);

            sw = new StreamWriter(OutputFolder + "/ContractList.csv");
            plsList = new List<ProductLookupSubscription>();

            sw.WriteLine("{0},{1},{2},{3},{4}", "InstrumentName",
                        "MarketKey",
                        "ProductType",
                        "ProductName",
                        "Volume");

            sw.Flush();
        }

        /// <summary>
        /// Create and start the Dispatcher
        /// </summary>
        public void Start()
        {
            // Attach a WorkerDispatcher to the current thread
            m_disp = Dispatcher.AttachWorkerDispatcher();
            m_disp.BeginInvoke(new Action(Init));
            m_disp.Run();
        }

        /// <summary>
        /// Initialize TT API
        /// </summary>
        public void Init()
        {
            // Use "Universal Login" Login Mode
            ApiInitializeHandler h = new ApiInitializeHandler(ttApiInitComplete);
            TTAPI.CreateUniversalLoginTTAPI(Dispatcher.Current, m_username, m_password, h);
        }

        /// <summary>
        /// Event notification for status of TT API initialization
        /// </summary>
        public void ttApiInitComplete(TTAPI api, ApiCreationException ex)
        {
            if (ex == null)
            {
                // Authenticate your credentials
                m_apiInstance = (UniversalLoginTTAPI)api;
                m_apiInstance.AuthenticationStatusUpdate += new EventHandler<AuthenticationStatusUpdateEventArgs>(apiInstance_AuthenticationStatusUpdate);
                m_apiInstance.Start();
            }
            else
            {
                Console.WriteLine("TT API Initialization Failed: {0}", ex.Message);
                Dispose();
            }
        }

        /// <summary>
        /// Event notification for status of authentication
        /// </summary>
        /// 
       
        public void apiInstance_AuthenticationStatusUpdate(object sender, AuthenticationStatusUpdateEventArgs e)
        {
            if (e.Status.IsSuccess)
            {
                
                // lookup an instrument
                foreach (string TickerHead in TickerheadList)
                {
                    string TickerHead2 = TickerHead;
                    string exchange = ContractUtilities.ContractMetaInfo.GetExchange4Tickerhead(TickerHead2);
                    if (exchange=="CME")
                        mkey = MarketKey.Cme;
                    else if  (exchange=="ICE")
                        mkey = MarketKey.Ice;

                    Console.WriteLine(TA.TickerheadConverters.ConvertFromDB2TT(TickerHead2));
                    ProductLookupSubscription pls = new ProductLookupSubscription(m_apiInstance.Session, Dispatcher.Current,
                        new ProductKey(mkey, ProductType.Future, TA.TickerheadConverters.ConvertFromDB2TT(TickerHead)));

                    plsList.Add(pls);
                    pls.Update += new EventHandler<ProductLookupSubscriptionEventArgs>(pls_Update);
                    pls.Start();

                    pls = new ProductLookupSubscription(m_apiInstance.Session, Dispatcher.Current,
                        new ProductKey(mkey, ProductType.Spread, TA.TickerheadConverters.ConvertFromDB2TT(TickerHead)));

                    plsList.Add(pls);
                    pls.Update += new EventHandler<ProductLookupSubscriptionEventArgs>(pls_Update);
                    pls.Start();
                }

            }
            else
            {
                Console.WriteLine("TT Login failed: {0}", e.Status.StatusMessage);
                Dispose();
            }
        }

        //TA.TickerheadConverters.ConvertFromDB2TT(TickerHead)
      
        void pls_Update(object sender, ProductLookupSubscriptionEventArgs e)
        {
            if (e.Error == null)
            {
                Console.WriteLine("Product Found: {0}", e.Product.Name);
                InstrumentCatalogSubscription ics = new InstrumentCatalogSubscription(e.Product, Dispatcher.Current);
                ics.InstrumentsUpdated += new EventHandler<InstrumentCatalogUpdatedEventArgs>(ics_InstrumentsUpdated);
                ics.Start();
            }
            else
            {
                Console.WriteLine(e.Error.Message);
            }
        }

        void ics_InstrumentsUpdated(object sender, InstrumentCatalogUpdatedEventArgs e)
        {
            foreach (TradingTechnologies.TTAPI.Instrument inst in e.Added)
            {
                Console.WriteLine("Instr: {0}", inst.Name);
                
                //Console.WriteLine(e.GetType());
                //sw.WriteLine(inst.Name);
                //sw.Flush();

                InstrumentList.Add(inst.Name);



                m_req = new InstrumentLookupSubscription(m_apiInstance.Session, Dispatcher.Current,inst.Key);
                m_req.Update += new EventHandler<InstrumentLookupSubscriptionEventArgs>(m_req_Update);
                m_req.Start();

                InstrumentCatalogSubscription ics2 = (InstrumentCatalogSubscription)sender;

                ics2.InstrumentsUpdated -= ics_InstrumentsUpdated;
                ics2.Dispose();
                ics2 = null;
                
            }

            
        }

        /// <summary>
        /// Event notification for instrument lookup
        /// </summary>
        void m_req_Update(object sender, InstrumentLookupSubscriptionEventArgs e)
        {
            if (e.Instrument != null && e.Error == null)
            {
                // Instrument was found
                Console.WriteLine("Found: {0}", e.Instrument.Name);

                // Subscribe for Inside Market Data
                m_ps = new PriceSubscription(e.Instrument, Dispatcher.Current);
                m_ps.Settings = new PriceSubscriptionSettings(PriceSubscriptionType.InsideMarket);
                m_ps.FieldsUpdated += new FieldsUpdatedEventHandler(m_ps_FieldsUpdated);
                m_ps.Start();
            }
            else if (e.IsFinal)
            {
                // Instrument was not found and TT API has given up looking for it
                Console.WriteLine("Cannot find instrument: {0}", e.Error.Message);
                Dispose();
            }
        }

        void m_ps_FieldsUpdated(object sender, FieldsUpdatedEventArgs e)
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
            lock (m_lock)
            {
                if (!m_disposed)
                {
                    // Unattached callbacks and dispose of all subscriptions
                    if (plsList != null)
                    {
                        for (int i = 0; i < plsList.Count; i++ )
                        {
                            plsList[i].Update -= pls_Update;
                            plsList[i].Dispose();
                            plsList[i] = null;
                        }
                        
                    }
                    if (ics != null)
                    {
                        ics.InstrumentsUpdated -= ics_InstrumentsUpdated;
                        ics.Dispose();
                        ics = null;
                    }

                    // Begin shutdown the TT API
                    TTAPI.ShutdownCompleted += new EventHandler(TTAPI_ShutdownCompleted);
                    TTAPI.Shutdown();

                    m_disposed = true;
                }
            }
        }

        /// <summary>
        /// Event notification for completion of TT API shutdown
        /// </summary>
        public void TTAPI_ShutdownCompleted(object sender, EventArgs e)
        {
            // Shutdown the Dispatcher
            if (m_disp != null)
            {
                m_disp.BeginInvokeShutdown();
                m_disp = null;
            }

            // Dispose of any other objects / resources
        }
    }
    }
