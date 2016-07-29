using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

using TradingTechnologies.TTAPI;
using TradingTechnologies.TTAPI.WinFormsHelpers;
using TA;

namespace WindowsFormsApplication1
{
    public partial class Form1 : Form
    {

        // Declare the TTAPI objects.
        private UniversalLoginTTAPI m_apiInstance = null;
        private InstrumentLookupSubscription m_req = null;
        private PriceSubscription m_ps = null;
        private bool m_isShutdown = false, m_shutdownInProcess = false;
        string[] tickerHeadList = new string[] { "ES", "EC", "CL" };
        int[] candleStartTimeList = new int[] { 830, 900, 930, 1000, 1030, 1100, 1130, 1200, 1230, 1300, 1330, 1400, 1430, 1500, 1530, 1600, 1630};
        int[] candleEndTimeList = new int[] { 900, 930, 1000, 1030, 1100, 1130, 1200, 1230, 1300, 1330, 1400, 1430, 1500, 1530, 1600, 1630, 1700};



        private MarketKey mkey;
        private ProductType ptype;
        List<TA.ContractVolume> ContractVolumeList;
        List<ContractVolume> FilteredList;
        List<String> tickerList;

        Dictionary<string, string> tickerFields;
        Dictionary<string, string> bidPriceFields;
        Dictionary<string, string> askPriceFields;
        Dictionary<string, string> lowPriceFields;
        Dictionary<string, string> highPriceFields;

        Double bidPrice;
        Double askPrice;
        Double midPrice;
        string tickerHeadDB;

        DataTable candlestickData;

        public Form1()
        {
            InitializeComponent();

            ContractVolumeList = TA.LoadContractVolumeFile.GetContractVolumes().ToList();
            FilteredList = new List<ContractVolume>();
            tickerList = new List<String>();

            tickerFields = new Dictionary<string, string>();
            bidPriceFields = new Dictionary<string, string>();
            askPriceFields = new Dictionary<string, string>();
            lowPriceFields = new Dictionary<string, string>();
            highPriceFields = new Dictionary<string, string>();

            candlestickData = new DataTable();

            candlestickData.Columns.Add("start", typeof(int));
            candlestickData.Columns.Add("end", typeof(int));


            for (int i = 0; i < tickerHeadList.Length; i++)
            {
                FilteredList.Add(ContractVolumeList.Where(x => x.ProductName == TA.TickerheadConverters.ConvertFromDB2TT(tickerHeadList[i])
                    && x.ProductType == "FUTURE").OrderByDescending(x => x.Volume).FirstOrDefault());
                tickerList.Add(TA.TickerConverters.ConvertFromTTAPIFields2DB(FilteredList[i].ProductName, FilteredList[i].InstrumentName));

                tickerFields.Add(tickerHeadList[i], "textBox" + Convert.ToString(i + 1) + "1");
                bidPriceFields.Add(tickerHeadList[i], "textBox" + Convert.ToString(i + 1) + "2");
                askPriceFields.Add(tickerHeadList[i], "textBox" + Convert.ToString(i + 1) + "3");
                lowPriceFields.Add(tickerHeadList[i], "textBox" + Convert.ToString(i + 1) + "4");
                highPriceFields.Add(tickerHeadList[i], "textBox" + Convert.ToString(i + 1) + "5");

                candlestickData.Columns.Add(tickerHeadList[i] + "_high", typeof(double));
                candlestickData.Columns.Add(tickerHeadList[i] + "_low", typeof(double));
            }

            for (int i = 0; i < candleStartTimeList.Length; i++)
			{
			 candlestickData.Rows.Add();
                candlestickData.Rows[i][0] = candleStartTimeList[i];
                candlestickData.Rows[i][1] = candleEndTimeList[i];

                for (int j = 0; j < tickerHeadList.Length; j++)
                {
                    candlestickData.Rows[i][2*j+2] = Double.NaN;
                    candlestickData.Rows[i][2*j+3] = Double.NaN;
                }

			}
           
        }        private void tableLayoutPanel1_Paint(object sender, PaintEventArgs e)
        {
             
        }

        public void ttApiInitHandler(TTAPI api, ApiCreationException ex)
        {
            if (ex == null)
            {
                m_apiInstance = (UniversalLoginTTAPI)api;
                m_apiInstance.AuthenticationStatusUpdate += new EventHandler<AuthenticationStatusUpdateEventArgs>(apiInstance_AuthenticationStatusUpdate);
                m_apiInstance.Start();
            }
            else if (!ex.IsRecoverable)
            {
                MessageBox.Show("API Initialization Failed: " + ex.Message);
            }
        }

        void apiInstance_AuthenticationStatusUpdate(object sender, AuthenticationStatusUpdateEventArgs e)
        {
            if (e.Status.IsSuccess)
            {
                this.Enabled = true;
            }
            else
            {
                MessageBox.Show(String.Format("ConnectionStatusUpdate: {0}", e.Status.StatusMessage));
            }
        }

        public void shutdownTTAPI()
        {
            if (!m_shutdownInProcess)
            {
                // Dispose of all request objects
                if (m_ps != null)
                {
                    m_ps.FieldsUpdated -= m_ps_FieldsUpdated;
                    m_ps.Dispose();
                    m_ps = null;
                }

                TTAPI.ShutdownCompleted += new EventHandler(TTAPI_ShutdownCompleted);
                TTAPI.Shutdown();
            }

            // only run shutdown once
            m_shutdownInProcess = true;
        }

        public void TTAPI_ShutdownCompleted(object sender, EventArgs e)
        {
            m_isShutdown = true;
            Close();
        }

        protected override void OnFormClosing(FormClosingEventArgs e)
        {
            if (!m_isShutdown)
            {
                e.Cancel = true;
                shutdownTTAPI();
            }
            else
            {
                base.OnFormClosing(e);
            }
        }


        private void Form1_Load(object sender, EventArgs e)
        {
for (int i = 0; i < tickerHeadList.Length; i++)
           {
               this.Controls.Find(tickerFields[tickerHeadList[i]], true)[0].Text = tickerList[i];
            }





        }

        private void button1_Click(object sender, EventArgs e)
        {

            for (int i = 0; i < FilteredList.Count; i++)
            {
                if (FilteredList[i].MarketKey == "CME")
                    mkey = MarketKey.Cme;
                else if (FilteredList[i].MarketKey == "ICE_IPE")
                    mkey = MarketKey.Ice;

                if (FilteredList[i].ProductType == "FUTURE")
                    ptype = ProductType.Future;
                else if (FilteredList[i].ProductType == "SPREAD")
                    ptype = ProductType.Spread;

                m_req = new InstrumentLookupSubscription(m_apiInstance.Session, Dispatcher.Current,
                new ProductKey(mkey, ptype, FilteredList[i].ProductName),
                FilteredList[i].InstrumentName);

                m_req.Update += new EventHandler<InstrumentLookupSubscriptionEventArgs>(m_req_Update);
                m_req.Start();
            }

        }

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
            if (e.Error != null)
            {
                Console.WriteLine(String.Format("TT API FieldsUpdated Error: {0}", e.Error));
                return;
            }

            // Extract the values we want as Typed fields

            bidPrice = Convert.ToDouble(e.Fields.GetDirectBidPriceField().FormattedValue);
            askPrice = Convert.ToDouble(e.Fields.GetDirectAskPriceField().FormattedValue);

            midPrice = (bidPrice + askPrice) / 2;
            tickerHeadDB = TA.TickerheadConverters.ConvertFromTT2DB(e.Fields.Instrument.Product.ToString());

            this.Controls.Find(bidPriceFields[tickerHeadDB], true)[0].Text = 
                e.Fields.GetDirectBidPriceField().FormattedValue;
            this.Controls.Find(askPriceFields[tickerHeadDB], true)[0].Text =
                e.Fields.GetDirectAskPriceField().FormattedValue;
                
            int hourMinute = 100 * DateTime.Now.Hour + DateTime.Now.Minute;

            int timeIndex = Enumerable.Range(0, candleStartTimeList.Length).Where(i => candleStartTimeList[i] > hourMinute).ToList()[0];

            double highPrice = (double)candlestickData.Rows[timeIndex][tickerHeadDB + "_high"];
            double lowPrice = (double)candlestickData.Rows[timeIndex][tickerHeadDB + "_low"];

            
            candlestickData.Rows[timeIndex][tickerHeadDB + "_low"] = midPrice;

            if ((Double.IsNaN(highPrice)) || (midPrice>highPrice))
            {
                candlestickData.Rows[timeIndex][tickerHeadDB + "_high"] = midPrice;
            }

            if ((Double.IsNaN(lowPrice)) || (midPrice < lowPrice))
            {
                candlestickData.Rows[timeIndex][tickerHeadDB + "_low"] = midPrice;
            }

            this.Controls.Find(lowPriceFields[tickerHeadDB], true)[0].Text =
                candlestickData.Rows[timeIndex][tickerHeadDB + "_low"].ToString();
            this.Controls.Find(highPriceFields[tickerHeadDB], true)[0].Text =
                candlestickData.Rows[timeIndex][tickerHeadDB + "_high"].ToString();
  
        }

        private void tableLayoutPanel1_Paint_1(object sender, PaintEventArgs e)
        {

        }

        private void textBox12_TextChanged(object sender, EventArgs e)
        {

        }

        
    }
}
