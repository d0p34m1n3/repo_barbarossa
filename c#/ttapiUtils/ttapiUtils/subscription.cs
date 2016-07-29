using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TradingTechnologies.TTAPI;
using TA;



namespace ttapiUtils
{
    public class Subscription
    {
        public List<TA.ContractVolume> ttapiTickerList { set; get; }
        private MarketKey mkey;
        private ProductType ptype;
        public UniversalLoginTTAPI m_apiInstance { set; get; }
        public WorkerDispatcher m_disp { set; get; }

        private bool m_disposed = false;
        private object m_lock = new object();

        private InstrumentLookupSubscription ils = null;
        private List<InstrumentLookupSubscription> ilsList = null;
        public EventHandler<InstrumentLookupSubscriptionEventArgs> ils_update { get; set; }
        public EventHandler<AuthenticationStatusUpdateEventArgs> asu_update { get; set; }
        public FieldsUpdatedEventHandler priceUpdatedEventHandler { get; set; }
        private PriceSubscription ps = null;
        private List<PriceSubscription> psList = null;
        private string m_username = "";
        private string m_password = "";


        public Subscription(string u, string p)
        {
            ilsList = new List<InstrumentLookupSubscription>();
            psList = new List<PriceSubscription>();
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
                Dispose1();
            }
        }

        
        public void startInstrumentLookupSubscriptions(object sender, AuthenticationStatusUpdateEventArgs e)
        {
            if (e.Status.IsSuccess)
            {
                foreach (ContractVolume cv in ttapiTickerList)
                {

                    if (cv.MarketKey == "CME")
                        mkey = MarketKey.Cme;
                    else if (cv.MarketKey == "ICE_IPE")
                        mkey = MarketKey.Ice;

                    if (cv.ProductType == "FUTURE")
                        ptype = ProductType.Future;
                    else if (cv.ProductType == "SPREAD")
                        ptype = ProductType.Spread;

                    ils = new InstrumentLookupSubscription(m_apiInstance.Session, Dispatcher.Current,
                    new ProductKey(mkey, ptype, cv.ProductName),
                    cv.InstrumentName);

                    ils.Update += new EventHandler<InstrumentLookupSubscriptionEventArgs>(ils_update);
                    ils.Start();
                    ilsList.Add(ils);
                }
            }
            else
            {
                Console.WriteLine("TT Login failed: {0}", e.Status.StatusMessage);
                Dispose1();
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
                ps.Start();
                psList.Add(ps);
            }
            else if (e.IsFinal)
            {
                // Instrument was not found and TT API has given up looking for it
                Console.WriteLine("Cannot find instrument: {0}", e.Error.Message);
                Dispose1();
            }
        }

        public void Dispose1()
        {
            lock (m_lock)
            {
                if (!m_disposed)
                {
                    if (ilsList != null)
                    {
                        for (int i = 0; i < ilsList.Count; i++)
                        {
                            ilsList[i].Update -= ils_update;
                            ilsList[i].Dispose();
                            ilsList[i] = null;
                        }
                        ilsList = null;
                    }

                    if (psList != null)
                    {
                        for (int i = 0; i < psList.Count; i++)
                        {
                            psList[i].FieldsUpdated -= priceUpdatedEventHandler;
                            psList[i].Dispose();
                            psList[i] = null;
                        }
                        psList = null;
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
