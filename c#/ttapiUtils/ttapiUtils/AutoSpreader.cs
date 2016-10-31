using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TA;
using TradingTechnologies.TTAPI;
using TradingTechnologies.TTAPI.Autospreader;
using TradingTechnologies.TTAPI.Tradebook;

namespace ttapiUtils
{
    public class AutoSpreader
    {
        public UniversalLoginTTAPI m_apiInstance { set; get; }
        public Subscription Subs;
        private InstrumentLookupSubscription Ils = null;
        private CreateAutospreaderInstrumentRequest Air = null;
        public Dictionary<InstrumentKey, InstrumentLookupSubscription> IlsDictionary;
        public Dictionary<InstrumentKey, Instrument> InstrumentDictionary;
        public List<EventHandler<InstrumentLookupSubscriptionEventArgs>> ilsUpdateList { get; set; }
        public List<string> DbTickerList { set; get; }
        public string OrderKey { set; get; }
        private List<string> TickerHeadList;
        public Dictionary<string, List<double>> RatioAndMultiplier;
        public string AutoSpreaderTickerHead;
        public string AutoSpreaderName;
        private MarketKey mkey;
        private ProductType ptype;
        private Dictionary<int, Instrument> SpreadLegKeys = new Dictionary<int, Instrument>();
        private List<string> ASEGTickerHeadList;
        private List<string> ASEZTickerHeadList;
        public string GateWay;
        private PriceSubscription ps = null;
        public ASInstrumentTradeSubscription ts = null;
        private int PayUpTicks;

        public AutoSpreader(List<string> dbTickerList,int payUpTicks=0)
        {
            DbTickerList = dbTickerList;
            PayUpTicks = payUpTicks;
            TickerHeadList = new List<string>();
            IlsDictionary = new Dictionary<InstrumentKey, InstrumentLookupSubscription>();
            InstrumentDictionary = new Dictionary<InstrumentKey, Instrument>();

            foreach (string item in dbTickerList)
            {
                if  (item.Contains("-"))
                {
                    string[] TickerListAux = item.Split('-');
                    TickerHeadList.Add(ContractUtilities.ContractMetaInfo.GetContractSpecs(TickerListAux[0]).tickerHead);
                    TickerHeadList.Add(ContractUtilities.ContractMetaInfo.GetContractSpecs(TickerListAux[1]).tickerHead);
                }
                else
                {
                    TickerHeadList.Add(ContractUtilities.ContractMetaInfo.GetContractSpecs(item).tickerHead);
                }
            }

            AutoSpreaderTickerHead = String.Join("_", TickerHeadList);
            AutoSpreaderName = String.Join("_", DbTickerList);
            RatioAndMultiplier = GetRatioAndMultiplierFromTickerHead(AutoSpreaderTickerHead);

            ASEGTickerHeadList = new List<string> { "CL", "RB", "HO", "NG" };
            ASEZTickerHeadList = new List<string> { "C", "W", "S", "KW", "SM", "BO" };
            GateWay = GetGateWay(TickerHeadList);
        }

        public Dictionary<string,List<double>> GetRatioAndMultiplierFromTickerHead(string tickerHead)
        {
            List<double> Ratio = null;
            List<double> Multiplier = null;
            Dictionary<string, List<double>> DictionaryOut = new Dictionary<string, List<double>>();

            if ((tickerHead=="CL_HO")||(tickerHead=="CL_RB"))
            {
                Ratio = new List<double> {1, -1};
                Multiplier = new List<double> {1, -0.42};
            }
            else if ((tickerHead=="HO_CL")||(tickerHead=="RB_CL"))
            {
                Ratio = new List<double> { 1, -1 };
                Multiplier = new List<double> { 0.42,-1};
            }
            else if (tickerHead=="CL_B")
            {
                Ratio = new List<double> { 1, -1 };
                Multiplier = new List<double> { 1, -100 };
            }
            else if (tickerHead == "B_CL")
            {
                Ratio = new List<double> { 1, -1 };
                Multiplier = new List<double> { 100, -1 };
            }
            else if ((tickerHead == "C_W") || (tickerHead == "W_C") || (tickerHead == "W_KW") || (tickerHead == "KW_W") ||
                     (tickerHead == "LN_LN_LN_LN") || (tickerHead == "LC_LC_LC_LC") || (tickerHead == "FC_FC_FC_FC") ||
                     (tickerHead == "C_C_C_C") || (tickerHead == "S_S_S_S") || (tickerHead == "SM_SM_SM_SM") ||
                     (tickerHead == "BO_BO_BO_BO") || (tickerHead == "W_W_W_W") || (tickerHead == "KW_KW_KW_KW") ||
                     (tickerHead == "SB_SB_SB_SB") || (tickerHead == "KC_KC_KC_KC") || (tickerHead == "CC_CC_CC_CC") ||
                     (tickerHead == "CT_CT_CT_CT") || (tickerHead == "OJ_OJ_OJ_OJ") ||
                     (tickerHead == "CL_CL_CL_CL") || (tickerHead == "B_B_B_B") || (tickerHead == "HO_HO_HO_HO") ||
                     (tickerHead == "RB_RB_RB_RB") || (tickerHead == "NG_NG_NG_NG") || (tickerHead == "ED_ED_ED_ED"))
            {
                Ratio = new List<double> { 1, -1 };
                Multiplier = new List<double> { 1, -1 };
            }
            else if (tickerHead=="S_BO_SM")
            {
                Ratio = new List<double> { 1, -1, -1};
                Multiplier = new List<double> { 100, -0.11, -0.22};
            }
           
            DictionaryOut.Add("Ratio", Ratio);
            DictionaryOut.Add("Multiplier", Multiplier);

            return DictionaryOut;

        }

        public string GetGateWay(List<string>TickerHeadList)
        {
            if (!TickerHeadList.Except(ASEGTickerHeadList).Any())
            {
                return "ASE-G";
            }
            else if (!TickerHeadList.Except(ASEZTickerHeadList).Any())
            {
                return "ASE-Z";
            }
            else
            {
                return "ASE";
            }
        }


        public void StartASEEventChain()
        {
                int TagValue = 1;

                foreach (string dbTicker in DbTickerList)
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

                    Ils.Tag = TagValue;
                    SpreadLegKeys.Add(TagValue, null);
                    TagValue++;

                    Ils.Update += new EventHandler<InstrumentLookupSubscriptionEventArgs>(CreateAutoSpreader);
                    
                    IlsDictionary.Add(IKey, Ils);
                    Ils.Start();
                }
        }

        private bool HaveWeFoundAllLegs()
        {
            if (SpreadLegKeys.Count == 0)
            {
                return false;
            }

            foreach (Instrument instrument in SpreadLegKeys.Values)
            {
                if (instrument == null)
                {
                    return false;
                }
            }

            return true;
        }

        void CreateAutoSpreader(object sender, InstrumentLookupSubscriptionEventArgs e)
        {
            if (e.Instrument != null && e.Error == null)
            {
                // Instrument was found
                Console.WriteLine("Found: {0}", e.Instrument.Name);

                InstrumentDictionary.Add(e.Instrument.Key, e.Instrument);

                // Update the dictionary to indicate that the instrument was found.
                InstrumentLookupSubscription instrLookupSub = sender as InstrumentLookupSubscription;

                if (SpreadLegKeys.ContainsKey((int)instrLookupSub.Tag))
                {
                    SpreadLegKeys[(int)instrLookupSub.Tag] = e.Instrument;
                }
            }
            else if (e.IsFinal)
            {
                // Instrument was not found and TT API has given up looking for it
                Console.WriteLine("Cannot find instrument: {0}", e.Error.Message);
                Subs.Dispose();
            }

            // If we have found all of the leg instruments, proceed with the creation of the spread.
            if (HaveWeFoundAllLegs())
            {
                Console.WriteLine("All leg instruments have been found.  Creating the spread...");

                // SpreadDetails related properties
                SpreadDetails spreadDetails = new SpreadDetails();
                spreadDetails.Name = AutoSpreaderName;

                for (int i = 0; i < DbTickerList.Count; i++)
                {
                    Instrument instrument = SpreadLegKeys[i + 1];
                    SpreadLegDetails spreadlegDetails = new SpreadLegDetails(instrument, instrument.GetValidOrderFeeds()[0].ConnectionKey);
                    spreadlegDetails.SpreadRatio = (int)RatioAndMultiplier["Ratio"][i];
                    spreadlegDetails.PriceMultiplier = RatioAndMultiplier["Multiplier"][i];
                    spreadlegDetails.CustomerName = "<DEFAULT>";
                    spreadlegDetails.PayupTicks = PayUpTicks;
                    spreadDetails.Legs.Append(spreadlegDetails);
                }

                // Create an AutospreaderInstrument corresponding to the synthetic spread
                Air = new CreateAutospreaderInstrumentRequest(m_apiInstance.Session, Dispatcher.Current, spreadDetails);
                Air.Completed += new EventHandler<CreateAutospreaderInstrumentRequestEventArgs>(m_casReq_Completed);
                Air.Submit();
            }
        }

        public void m_casReq_Completed(object sender, CreateAutospreaderInstrumentRequestEventArgs e)
        {
            if (e.Error == null)
            {
                if (e.Instrument != null)
                {
                    // In this example, the AutospreaderInstrument is launched to ASE-A.
                    // You should use the order feed that is appropriate for your purposes.
                    OrderFeed oFeed = this.GetOrderFeedByName(e.Instrument, GateWay);
                    if (oFeed.IsTradingEnabled)
                    {
                        // deploy the Autospreader Instrument to the specified ASE
                        e.Instrument.TradableStatusChanged += new EventHandler<TradableStatusChangedEventArgs>(Instrument_TradableStatusChanged);
                        LaunchReturnCode lrc = e.Instrument.LaunchToOrderFeed(oFeed);
                        if (lrc != LaunchReturnCode.Success)
                        {
                            Console.WriteLine("Launch request was unsuccessful");
                        }
                    }
                }
            }
            else
            {
                // AutospreaderInstrument creation failed
                Console.WriteLine("AutospreaderInstrument creation failed: " + e.Error.Message);
            }
        }

        OrderFeed GetOrderFeedByName(Instrument instr, string gateway)
        {
            foreach (OrderFeed oFeed in instr.GetValidOrderFeeds())
            {
                if (oFeed.Name.Equals(gateway))
                {
                    return oFeed;
                }
            }

            return (OrderFeed)null;
        }

        void Instrument_TradableStatusChanged(object sender, TradableStatusChangedEventArgs e)
        {
            if (e.Value)
            {
                // launch of AutospreaderInstrument was successful
                AutospreaderInstrument instr = sender as AutospreaderInstrument;

                // Subscribe for Inside Market Data
                ps = new PriceSubscription(instr, Dispatcher.Current);
                ps.Settings = new PriceSubscriptionSettings(PriceSubscriptionType.InsideMarket);
                ps.FieldsUpdated += new FieldsUpdatedEventHandler(Subs.priceUpdatedEventHandler);
                ps.Start();

                // Create an ASTradeSubscription to listen for order / fill events only for orders submitted through it
                ts = new ASInstrumentTradeSubscription(m_apiInstance.Session, Dispatcher.Current, instr, true, true, false, false);
                ts.OrderUpdated += new EventHandler<OrderUpdatedEventArgs>(m_ts_OrderUpdated);
                ts.OrderAdded += new EventHandler<OrderAddedEventArgs>(m_ts_OrderAdded);
                ts.OrderDeleted += new EventHandler<OrderDeletedEventArgs>(m_ts_OrderDeleted);
                ts.OrderFilled += new EventHandler<OrderFilledEventArgs>(Subs.orderFilledEventHandler);
                ts.OrderRejected += new EventHandler<OrderRejectedEventArgs>(m_ts_OrderRejected);
                ts.Start();
            }
            else
            {
                Console.WriteLine("Launch of AutospreaderInstrument to {0} was unsuccessful.", e.OrderFeed.Name);
            }
        }

        void m_ts_OrderRejected(object sender, OrderRejectedEventArgs e)
        {
            if (e.Order.SiteOrderKey == OrderKey)
            {
                // Our parent order has been rejected
                Console.WriteLine("Our parent order has been rejected: {0}", e.Message);
            }
            else if (e.Order.SyntheticOrderKey == OrderKey)
            {
                // A child order of our parent order has been rejected
                Console.WriteLine("A child order of our parent order has been rejected: {0}", e.Message);
            }
        }

        /// <summary>
        /// Event notification for order filled
        /// </summary>
        void m_ts_OrderFilled(object sender, OrderFilledEventArgs e)
        {
            if (e.Fill.SiteOrderKey == OrderKey)
            {
                // Our parent order has been filled
                Console.WriteLine("Our parent order has been " + (e.Fill.FillType == FillType.Full ? "fully" : "partially") + " filled");
            }
            else if (e.Fill.ParentKey == OrderKey)
            {
                // A child order of our parent order has been filled
                Console.WriteLine("A child order of our parent order has been " + (e.Fill.FillType == FillType.Full ? "fully" : "partially") + " filled");
            }

            Console.WriteLine("Average Buy Price = {0} : Net Position = {1} : P&L = {2}", ts.ProfitLossStatistics.BuyAveragePrice,
                ts.ProfitLossStatistics.NetPosition, ts.ProfitLoss.AsPrimaryCurrency);
        }

        /// <summary>
        /// Event notification for order deleted
        /// </summary>
        void m_ts_OrderDeleted(object sender, OrderDeletedEventArgs e)
        {
            if (e.DeletedUpdate.SiteOrderKey == OrderKey)
            {
                // Our parent order has been deleted
                Console.WriteLine("Our parent order has been deleted: {0}", e.Message);
            }
            else if (e.DeletedUpdate.SyntheticOrderKey == OrderKey)
            {
                // A child order of our parent order has been deleted
                Console.WriteLine("A child order of our parent order has been deleted: {0}", e.Message);
            }
        }

        /// <summary>
        /// Event notification for order added
        /// </summary>
        void m_ts_OrderAdded(object sender, OrderAddedEventArgs e)
        {
            if (e.Order.SiteOrderKey == OrderKey)
            {
                // Our parent order has been added
                Console.WriteLine("Our parent order has been added: {0}", e.Message);
            }
            else if (e.Order.SyntheticOrderKey == OrderKey)
            {
                // A child order of our parent order has been added
                Console.WriteLine("A child order of our parent order has been added: {0}", e.Message);
            }
        }

        /// <summary>
        /// Event notification for order update
        /// </summary>
        void m_ts_OrderUpdated(object sender, OrderUpdatedEventArgs e)
        {
            if (e.OldOrder.SiteOrderKey == OrderKey)
            {
                // Our parent order has been updated
                Console.WriteLine("Our parent order has been updated: {0}", e.Message);
            }
            else if (e.OldOrder.SyntheticOrderKey == OrderKey)
            {
                // A child order of our parent order has been updated
                Console.WriteLine("A child order of our parent order has been updated: {0}", e.Message);
            }
        }



    }
}
