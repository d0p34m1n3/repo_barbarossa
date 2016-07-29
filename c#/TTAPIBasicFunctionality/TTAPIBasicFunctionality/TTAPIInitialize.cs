using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TTAPIBasicFunctionality
{
    using TradingTechnologies.TTAPI;

    public class TTAPIInitialize: IDisposable
    {
        public UniversalLoginTTAPI apiInstance = null;
        public WorkerDispatcher m_disp = null;
        public string username = "";
        public string password = "";
        public bool APIInitializeSucces {get; set;}
        private object m_lock = new object();
        private bool m_disposed = false;
        private List<string> InstrumentList = new List<string>();

        /// <summary>
        /// Primary constructor
        /// </summary>
        public TTAPIInitialize(string u, string p)
        {
            username = u;
            password = p;
            Start();
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
            TTAPI.CreateUniversalLoginTTAPI(Dispatcher.Current, username, password, h);
        }

        public void ttApiInitComplete(TTAPI api, ApiCreationException ex)
        {
            if (ex == null)
            {
                // Authenticate your credentials
                apiInstance = (UniversalLoginTTAPI)api;
                apiInstance.AuthenticationStatusUpdate += new EventHandler<AuthenticationStatusUpdateEventArgs>(apiInstance_AuthenticationStatusUpdate);
                apiInstance.Start();
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

                ProductLookupSubscription pls = new ProductLookupSubscription(apiInstance.Session, Dispatcher.Current,
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
