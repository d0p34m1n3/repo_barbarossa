using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace quantTrader
{
    using TradingTechnologies.TTAPI;

    /// <summary>
    /// Main TT API class
    /// </summary>
    class TTAPIFunctions : IDisposable
    {
        /// <summary>
        /// Declare the TTAPI objects
        /// </summary>
        private UniversalLoginTTAPI m_apiInstance = null;
        private WorkerDispatcher m_disp = null;
        private bool m_disposed = false;
        private object m_lock = new object();
        private ProductLookupSubscription pls = null;
        private InstrumentCatalogSubscription ics = null;
        private List<string> InstrumentList = new List<string>();



        private string m_username = "";
        private string m_password = "";


        /// <summary>
        /// Private default constructor
        /// </summary>
        private TTAPIFunctions() 
        { 
        }

        /// <summary>
        /// Primary constructor
        /// </summary>
        public TTAPIFunctions(string u, string p)
        {
            m_username = u;
            m_password = p;
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
        public void apiInstance_AuthenticationStatusUpdate(object sender, AuthenticationStatusUpdateEventArgs e)
        {
            if (e.Status.IsSuccess)
            {
                
                ProductLookupSubscription pls = new ProductLookupSubscription(m_apiInstance.Session, Dispatcher.Current,
                    new ProductKey(MarketKey.Cme, ProductType.Future, "CL"));
                pls.Update += new EventHandler<ProductLookupSubscriptionEventArgs>(pls_Update);
                pls.Start();

            }
            else
            {
                Console.WriteLine("TT Login failed: {0}", e.Status.StatusMessage);
                Dispose();
            }
        }

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

                InstrumentList.Add(inst.Name);
                //Console.WriteLine(e.    );
                
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
                    if (pls != null)
                    {
                        pls.Update -= pls_Update;
                        pls.Dispose();
                        pls = null;
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
