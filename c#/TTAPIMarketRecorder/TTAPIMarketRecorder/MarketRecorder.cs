using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using CalendarUtilities;
using IOUtilities;
using System.Data;



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

    public class ContractVolume
    {
        public String InstrumentName { get; set;}
        public String MarketKey { get; set; }
        public String ProductType { get; set; }
        public String ProductName { get; set; }
        public int Volume { get; set; }

    }

    public sealed class MyClassMap : CsvClassMap<ContractVolume>
    {
        public MyClassMap()
        {
            Map(m => m.InstrumentName);
            Map(m => m.MarketKey);
            Map(m => m.ProductType);
            Map(m => m.ProductName);
            Map(m => m.Volume).Default(0);
        }
    }

    class MarketRecorder : IDisposable
    {
        /// <summary>
        /// Declare the TTAPI objects
        /// </summary>
        private UniversalLoginTTAPI m_apiInstance = null;
        private WorkerDispatcher m_disp = null;
        private bool m_disposed = false;
        private object m_lock = new object();
        private InstrumentLookupSubscription m_req = null;
        private PriceSubscription m_ps = null;
        private string m_username = "";
        private string m_password = "";
        private string ContractListFolder;
        private string OutputFolder;
        private StreamWriter sw;
        private MarketKey mkey;
        private ProductType ptype;
        IEnumerable<ContractVolume> ContractVolumeList;
        List<ContractVolume> FilteredList;
        DataTable ContractVolumeTable;
        Dictionary<string, StreamWriter> PriceFileDictionary;

        //StringBuilder sb = new StringBuilder();

        public static DataTable ToDataTable<T>(List<T> items)
        {
            DataTable dataTable = new DataTable(typeof(T).Name);

            //Get all the properties
            PropertyInfo[] Props = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);
            foreach (PropertyInfo prop in Props)
            {
                //Setting column names as Property names
                dataTable.Columns.Add(prop.Name);
            }
            foreach (T item in items)
            {
                var values = new object[Props.Length];
                for (int i = 0; i < Props.Length; i++)
                {
                    //inserting property values to datatable rows
                    values[i] = Props[i].GetValue(item, null);
                }
                dataTable.Rows.Add(values);
            }
            //put a breakpoint here and check datatable
            return dataTable;
        }


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

            ContractListFolder = "C:/Research/data/intraday_data/tt_api/" + BusinessDays.GetDirectoryExtension(BusinessDays.GetBusinessDayShifted(-1));
            OutputFolder = "C:/Research/data/intraday_data/tt_api/" + BusinessDays.GetDirectoryExtension(DateTime.Now.Date);

            System.IO.Directory.CreateDirectory(OutputFolder);

            PriceFileDictionary = new Dictionary<string, StreamWriter>();
            FilteredList = new List<ContractVolume>();


           // using (var sr = new StreamReader(ContractListFolder + "/ContractList.csv"))
           // {

            var sr = new StreamReader(ContractListFolder + "/ContractList.csv");
                var reader = new CsvHelper.CsvReader(sr);
                reader.Configuration.RegisterClassMap(new MyClassMap());


                ContractVolumeList = reader.GetRecords<ContractVolume>();

                FilteredList = ContractVolumeList.Where(x => x.Volume > 100).ToList();

                for (int i = 0; i < FilteredList.Count;  i++)
                {
                    Console.WriteLine(FilteredList[i].InstrumentName);
                    //sw = new StreamWriter(OutputFolder + "/" + cv.InstrumentName + ".csv");
                    PriceFileDictionary.Add(FilteredList[i].InstrumentName, new StreamWriter(OutputFolder + "/" + FilteredList[i].InstrumentName.Replace("/", "-").Replace(":", "-") + ".csv"));
                }

                //FilteredList = ContractVolumeList.Where(x => x.Volume > 100).ToList();

           // }

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
                
                foreach (ContractVolume cv in FilteredList)
                {
                    
                    if (cv.MarketKey == "CME")
                        mkey = MarketKey.Cme;
                    else if (cv.MarketKey == "ICE_IPE")
                        mkey = MarketKey.Ice;

                    if (cv.ProductType == "FUTURE")
                        ptype = ProductType.Future;
                    else if (cv.ProductType == "SPREAD")
                        ptype = ProductType.Spread;

                    m_req = new InstrumentLookupSubscription(m_apiInstance.Session, Dispatcher.Current,
                    new ProductKey(mkey, ptype, cv.ProductName),
                    cv.InstrumentName);

                    m_req.Update += new EventHandler<InstrumentLookupSubscriptionEventArgs>(m_req_Update);
                    m_req.Start();
                }

               
            }
            else
            {
                Console.WriteLine("TT Login failed: {0}", e.Status.StatusMessage);
                Dispose();
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

        /// <summary>
        /// Event notification for price update
        /// </summary>
        void m_ps_FieldsUpdated(object sender, FieldsUpdatedEventArgs e)
        {
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
                        //PriceFileDictionary[e.Fields.Instrument.Name.ToString()].Flush();

                        //Console.WriteLine("    {0} : {1}", id.ToString(), e.Fields[id].FormattedValue);
                        //sw.WriteLine("{0},{1}", id.ToString(), e.Fields[id].FormattedValue);
                        //sw.Flush();
                    }
                }
                else
                {
                    // Only some fields have changed
                    //Console.WriteLine("Market Data Update:");

                    foreach (FieldId id in e.Fields.GetChangedFieldIds())
                    {
                        //Console.WriteLine("    {0} : {1}", id.ToString(), e.Fields[id].FormattedValue);

                        PriceFileDictionary[e.Fields.Instrument.Name.ToString()].WriteLine("{0},{1},{2}", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"),id.ToString(), e.Fields[id].FormattedValue);
                        //PriceFileDictionary[e.Fields.Instrument.Name.ToString()].Flush();

                        //sw.WriteLine("{0},{1}",id.ToString(),e.Fields[id].FormattedValue);
                        //sw.Flush();
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
            lock (m_lock)
            {
                if (!m_disposed)
                {
                    // Unattached callbacks and dispose of all subscriptions
                    if (m_req != null)
                    {
                        m_req.Update -= m_req_Update;
                        m_req.Dispose();
                        m_req = null;
                    }
                    if (m_ps != null)
                    {
                        m_ps.FieldsUpdated -= m_ps_FieldsUpdated;
                        m_ps.Dispose();
                        m_ps = null;
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
