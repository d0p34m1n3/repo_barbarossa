using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TradingTechnologies.TTAPI;
using TradingTechnologies.TTAPI.Tradebook;
using TA;

namespace ttapiUtils
{
    public class Subscription
    {
        public List<string> dbTickerList { set; get; }
        public List<string> TickerHeadList { set; get;}
        public List<ttapiTicker> TTAPITickerList { set; get; }
        private MarketKey mkey;
        private ProductType ptype;
        public UniversalLoginTTAPI m_apiInstance { set; get; }
        public WorkerDispatcher m_disp { set; get; }

        private bool m_disposed = false;
        private object m_lock = new object();

        private InstrumentLookupSubscription Ils = null;
        private PriceSubscription ps = null;
        private InstrumentTradeSubscription ts = null;

        public Dictionary<ProductKey, ProductLookupSubscription> PlsDictionary;
        public Dictionary<ProductKey, InstrumentCatalogSubscription> IcsDictionary;
        public Dictionary<InstrumentKey, InstrumentLookupSubscription> IlsDictionary;
        public Dictionary<InstrumentKey, PriceSubscription> PsDictionary;
        public Dictionary<InstrumentKey, InstrumentTradeSubscription> TsDictionary;

        public EventHandler<AuthenticationStatusUpdateEventArgs> asu_update { get; set; }
        public EventHandler<ProductLookupSubscriptionEventArgs> PLSEventHandler { get; set; }
        public EventHandler<InstrumentCatalogUpdatedEventArgs> ICUEventHandler { get; set; }
        public List<EventHandler<InstrumentLookupSubscriptionEventArgs>> ilsUpdateList { get; set; }
        public FieldsUpdatedEventHandler priceUpdatedEventHandler { get; set; }
        public EventHandler<OrderFilledEventArgs> orderFilledEventHandler { get; set; }
        
        private string m_username = "";
        private string m_password = "";

        public bool PlsDictionaryCompleteQ;
        public bool IlsDictionaryCompleteQ;


        public Subscription(string u, string p)
        {
            IlsDictionaryCompleteQ = false;
            PlsDictionary = new Dictionary<ProductKey, ProductLookupSubscription>();
            IcsDictionary = new Dictionary<ProductKey, InstrumentCatalogSubscription>();
            IlsDictionary = new Dictionary<InstrumentKey, InstrumentLookupSubscription>();
            PsDictionary = new Dictionary<InstrumentKey, PriceSubscription>();
            TsDictionary = new Dictionary<InstrumentKey, InstrumentTradeSubscription>();
            m_username = u;
            m_password = p;
        }

        public void Start()
        {

            

            // Attach a WorkerDispatcher to the current thread
            m_disp = Dispatcher.AttachWorkerDispatcher();
            m_disp.BeginInvoke(new Action(Init));
            m_disp.Run();
   
        }

        public void Init()
        {
            // Use "Universal Login" Login Mode
            ApiInitializeHandler h = new ApiInitializeHandler(ttApiInitComplete);
            TTAPI.CreateUniversalLoginTTAPI(Dispatcher.Current, m_username, m_password, h);
        }

        public void ttApiInitComplete(TTAPI api, ApiCreationException ex)
        {
            if (ex == null)
            {
                // Authenticate your credentials
                m_apiInstance = (UniversalLoginTTAPI)api;
                m_apiInstance.AuthenticationStatusUpdate += new EventHandler<AuthenticationStatusUpdateEventArgs>(asu_update);
                m_apiInstance.Start();
            }
            else
            {
                Console.WriteLine("TT API Initialization Failed: {0}", ex.Message);
                Dispose();
            }
        }

        
        public void startInstrumentLookupSubscriptions(object sender, AuthenticationStatusUpdateEventArgs e)
        {
            if (e.Status.IsSuccess)
            {
                foreach (string dbTicker in dbTickerList)
                {
                    ttapiTicker currentTicker = TA.TickerConverters.ConvertFromDbTicker2ttapiTicker(dbTicker);

                    if (currentTicker.marketKey == "CME")
                        mkey = MarketKey.Cme;
                    else if (currentTicker.marketKey == "ICE_IPE")
                        mkey = MarketKey.Ice;

                    if (currentTicker.productType == "FUTURE")
                        ptype = ProductType.Future;
                    else if (currentTicker.productType == "SPREAD")
                        ptype = ProductType.Spread;
                    
                    InstrumentKey IKey = new InstrumentKey(new ProductKey(mkey, ptype, currentTicker.productName), currentTicker.SeriesKey);
    
                    Ils = new InstrumentLookupSubscription(m_apiInstance.Session, Dispatcher.Current, new ProductKey(mkey, ptype, currentTicker.productName), currentTicker.SeriesKey);

                    for (int i = 0; i < ilsUpdateList.Count; i++)
                    {
                        Ils.Update += new EventHandler<InstrumentLookupSubscriptionEventArgs>(ilsUpdateList[i]);
                    }

                    IlsDictionary.Add(IKey, Ils);
                    Ils.Start();
                }
            }
            else
            {
                Console.WriteLine("TT Login failed: {0}", e.Status.StatusMessage);
                Dispose();
            }
        }

        public void StartInstrumentLookupSubscriptionsFromCatalog(object sender, InstrumentCatalogUpdatedEventArgs e)
        {

            foreach (TradingTechnologies.TTAPI.Instrument inst in e.Added)
            {
                Console.WriteLine("Instr: {0}", inst.Name);


                Ils = new InstrumentLookupSubscription(m_apiInstance.Session, Dispatcher.Current, inst.Key);
                
                for (int i = 0; i < ilsUpdateList.Count; i++)
                {
                    Ils.Update += new EventHandler<InstrumentLookupSubscriptionEventArgs>(ilsUpdateList[i]);
                }

                Console.WriteLine(Ils.Instrument.Key.ToString());

                IlsDictionary.Add(inst.Key, Ils);
                Ils.Start();

                //InstrumentCatalogSubscription ics2 = (InstrumentCatalogSubscription)sender;

                //ics2.InstrumentsUpdated -= ics_InstrumentsUpdated;
                //ics2.Dispose();
                //ics2 = null;
            }

            TradingTechnologies.TTAPI.Product Prod = ((InstrumentCatalogSubscription)sender).Product;

            PlsDictionary[Prod.Key].Update -= PLSEventHandler;
            PlsDictionary[Prod.Key].Dispose();
            PlsDictionary[Prod.Key] = null;
            PlsDictionary.Remove(Prod.Key);

            if ((PlsDictionary.Count == 0) & PlsDictionaryCompleteQ)
            {
                IlsDictionaryCompleteQ = true;
            }


        }

        public void startInstrumentLookupSubscriptionsFromTTAPITickers(object sender, AuthenticationStatusUpdateEventArgs e)
        {
            if (e.Status.IsSuccess)
            {
                foreach (ttapiTicker ticker in TTAPITickerList)
                {

                    if (ticker.marketKey == "CME")
                        mkey = MarketKey.Cme;
                    else if (ticker.marketKey == "ICE_IPE")
                        mkey = MarketKey.Ice;

                    if (ticker.productType == "FUTURE")
                        ptype = ProductType.Future;
                    else if (ticker.productType == "SPREAD")
                        ptype = ProductType.Spread;

                    InstrumentKey IKey = new InstrumentKey(new ProductKey(mkey, ptype, ticker.productName), ticker.SeriesKey);
                    Ils = new InstrumentLookupSubscription(m_apiInstance.Session, Dispatcher.Current, new ProductKey(mkey, ptype, ticker.productName), ticker.instrumentName);

                    for (int i = 0; i < ilsUpdateList.Count; i++)
                    {
                        Ils.Update += new EventHandler<InstrumentLookupSubscriptionEventArgs>(ilsUpdateList[i]);
                    }

                    IlsDictionary.Add(IKey, Ils);
                    Ils.Start();

                }
            }
            else
            {
                Console.WriteLine("TT Login failed: {0}", e.Status.StatusMessage);
                Dispose();
            }
        }

        public void startProductLookupSubscriptions(object sender, AuthenticationStatusUpdateEventArgs e)
        {
            PlsDictionaryCompleteQ = false;

            if (e.Status.IsSuccess)
            {

                // lookup an instrument
                foreach (string TickerHead in TickerHeadList)
                {
                    string exchange = ContractUtilities.ContractMetaInfo.GetExchange4Tickerhead(TickerHead);
                    if (exchange == "CME")
                        mkey = MarketKey.Cme;
                    else if (exchange == "ICE")
                        mkey = MarketKey.Ice;

                    Console.WriteLine(TA.TickerheadConverters.ConvertFromDB2TT(TickerHead));

                    ProductKey ProdKey = new ProductKey(mkey, ProductType.Future, TA.TickerheadConverters.ConvertFromDB2TT(TickerHead));
                    ProductLookupSubscription PLS = new ProductLookupSubscription(m_apiInstance.Session, Dispatcher.Current, ProdKey);
                    PlsDictionary.Add(ProdKey, PLS);
                    PLS.Update += new EventHandler<ProductLookupSubscriptionEventArgs>(PLSEventHandler);
                    PLS.Start();

                    ProdKey = new ProductKey(mkey, ProductType.Spread, TA.TickerheadConverters.ConvertFromDB2TT(TickerHead));
                    PLS = new ProductLookupSubscription(m_apiInstance.Session, Dispatcher.Current, ProdKey);

                    PlsDictionary.Add(ProdKey, PLS);
                    PLS.Update += new EventHandler<ProductLookupSubscriptionEventArgs>(PLSEventHandler);
                    PLS.Start();
                }
                PlsDictionaryCompleteQ = true;
            }

            else
            {
                Console.WriteLine("TT Login failed: {0}", e.Status.StatusMessage);
                Dispose();
            }
        }

        public void Subscribe2InstrumentCatalogs(object sender, ProductLookupSubscriptionEventArgs e)
        {
            if (e.Error == null)
            {
                Console.WriteLine("Product Found: {0}", e.Product.Name);
                InstrumentCatalogSubscription ICS = new InstrumentCatalogSubscription(e.Product, Dispatcher.Current);

                IcsDictionary.Add(e.Product.Key, ICS);
                ICS.InstrumentsUpdated += new EventHandler<InstrumentCatalogUpdatedEventArgs>(ICUEventHandler);
                ICS.Start();
            }
            else
            {
                Console.WriteLine(e.Error.Message);
            }
        }


        public void startPriceSubscriptions(object sender, InstrumentLookupSubscriptionEventArgs e)
        {
            if (e.Instrument != null && e.Error == null)
            {
                // Instrument was found
                Console.WriteLine("Found: {0}", e.Instrument.Name);

                // Subscribe for Inside Market Data
                ps = new PriceSubscription(e.Instrument, Dispatcher.Current);
                ps.Settings = new PriceSubscriptionSettings(PriceSubscriptionType.InsideMarket);
                ps.FieldsUpdated += new FieldsUpdatedEventHandler(priceUpdatedEventHandler);
                PsDictionary.Add(e.Instrument.Key,ps);

                ps.Start();
                
            }
            else if (e.IsFinal)
            {
                // Instrument was not found and TT API has given up looking for it
                Console.WriteLine("Cannot find instrument: {0}", e.Error.Message);
                Dispose();
            }
        }

        public void startTradeSubscriptions(object sender, InstrumentLookupSubscriptionEventArgs e)
        {
            if (e.Instrument != null && e.Error == null)
            {
                // Create a TradeSubscription to listen for order / fill events only for orders submitted through it
                ts = new InstrumentTradeSubscription(m_apiInstance.Session, Dispatcher.Current, e.Instrument, true, true, false, false);
                ts.OrderUpdated += new EventHandler<OrderUpdatedEventArgs>(m_ts_OrderUpdated);
                ts.OrderAdded += new EventHandler<OrderAddedEventArgs>(m_ts_OrderAdded);
                ts.OrderDeleted += new EventHandler<OrderDeletedEventArgs>(m_ts_OrderDeleted);
                ts.OrderFilled += new EventHandler<OrderFilledEventArgs>(orderFilledEventHandler);
                ts.OrderRejected += new EventHandler<OrderRejectedEventArgs>(m_ts_OrderRejected);

                TsDictionary.Add(e.Instrument.Key,ts);
                ts.Start();
            }
            else if (e.IsFinal)
            {
                // Instrument was not found and TT API has given up looking for it
                Console.WriteLine("Cannot find instrument: {0}", e.Error.Message);
                Dispose();
            }
        }

        void m_ts_OrderRejected(object sender, OrderRejectedEventArgs e)
        {
            Console.WriteLine("Order was rejected.");
            Console.WriteLine(e.Message);
        }

        /// <summary>
        /// Event notification for order filled
        /// </summary>
       

        /// </summary>
        void m_ts_OrderDeleted(object sender, OrderDeletedEventArgs e)
        {
            Console.WriteLine("Order was deleted.");
        }

        /// <summary>
        /// Event notification for order added
        /// </summary>
        void m_ts_OrderAdded(object sender, OrderAddedEventArgs e)
        {
            Console.WriteLine("Order was added with price of {0}.", e.Order.LimitPrice);
        }

        void m_ts_OrderUpdated(object sender, OrderUpdatedEventArgs e)
        {
            Console.WriteLine("Order was updated with price of {0}.", e.NewOrder.LimitPrice);
        }

       
        public void Dispose()
        {
            lock (m_lock)
            {
                if (!m_disposed)
                {
                    if (PlsDictionary != null)
                    {
                        foreach (ProductKey Pkey in PlsDictionary.Keys.ToList())
                        {
                            PlsDictionary[Pkey].Update -= PLSEventHandler;
                            PlsDictionary[Pkey].Dispose();
                            PlsDictionary[Pkey] = null;
                        }
                        PlsDictionary = null;
                    }

                    if (IcsDictionary != null)
                    {
                        foreach (ProductKey Pkey in IcsDictionary.Keys.ToList())
                        {
                            IcsDictionary[Pkey].InstrumentsUpdated -= ICUEventHandler;
                            IcsDictionary[Pkey].Dispose();
                            IcsDictionary[Pkey] = null;
                        }
                        IcsDictionary = null;
                    }

                    if (IlsDictionary != null)
                    {

                        foreach (InstrumentKey IKey in IlsDictionary.Keys.ToList())
                        {
                            for (int j = 0; j < ilsUpdateList.Count; j++)
                            {
                                IlsDictionary[IKey].Update -= ilsUpdateList[j];
                            }
                            IlsDictionary[IKey].Dispose();
                            IlsDictionary[IKey] = null;
                        }
                        IlsDictionary = null;
                    }

                    if (PsDictionary !=null)
                    {
                        foreach (InstrumentKey Ikey in PsDictionary.Keys.ToList())
                        {
                            PsDictionary[Ikey].FieldsUpdated -= priceUpdatedEventHandler;
                            PsDictionary[Ikey].Dispose();
                            PsDictionary[Ikey] = null;
                        }
                        PsDictionary = null;
                    }

                    if (TsDictionary != null)
                    {
                        foreach (InstrumentKey IKey in TsDictionary.Keys.ToList())
                        {
                            TsDictionary[IKey].OrderUpdated -= m_ts_OrderUpdated;
                            TsDictionary[IKey].OrderAdded -= m_ts_OrderAdded;
                            TsDictionary[IKey].OrderDeleted -= m_ts_OrderDeleted;
                            TsDictionary[IKey].OrderFilled -= orderFilledEventHandler;
                            TsDictionary[IKey].OrderRejected -= m_ts_OrderRejected;
                            TsDictionary[IKey].Dispose();
                            TsDictionary[IKey] = null;
                        }
                        TsDictionary = null;
                    }

                       
                    // Begin shutdown the TT API
                    TTAPI.ShutdownCompleted += new EventHandler(TTAPI_ShutdownCompleted);
                    TTAPI.Shutdown();
                    m_disposed = true;
                }
            }

        }

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
