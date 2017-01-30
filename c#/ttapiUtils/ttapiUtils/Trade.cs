using Shared;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TradingTechnologies.TTAPI;
using TradingTechnologies.TTAPI.Autospreader;
using TradingTechnologies.TTAPI.Tradebook;

namespace ttapiUtils
{
    public static class Trade
    {
        public static string SendLimitOrder(TradingTechnologies.TTAPI.Instrument instrument,
            TradingTechnologies.TTAPI.Price price, int qty, Subscription ttapisubs, string orderTag="")

        {
            BuySell Direction = BuySell.Buy;
            string AccountName = "H1KOC";

            if (qty<0)
            {
                Direction = BuySell.Sell;
                qty = -qty;
            }

            string tickerDB = TA.TickerConverters.ConvertFromTTAPIFields2DB(instrument.Product.ToString(), instrument.Name.ToString());
            string[] TickerList = tickerDB.Split('-');

            string TickerHead = ContractUtilities.ContractMetaInfo.GetContractSpecs(TickerList[0]).tickerHead;
            string ExchangeName = ContractUtilities.ContractMetaInfo.GetExchange4Tickerhead(TickerHead);

            AccountType AccType = AccountType.P2;

            OrderProfile op = new OrderProfile(instrument.GetValidOrderFeeds()[0], instrument);
            op.BuySell = Direction;
            op.AccountName = AccountName;

            if (ExchangeName == "ICE")
            {
                AccType = AccountType.G2;
                op.GiveUp = "5283";
            }

            op.AccountType = AccType;
            op.OrderQuantity = Quantity.FromInt(instrument, qty);
            op.OrderType = OrderType.Limit;

            if (orderTag.Count()>0)
            {
                op.OrderTag = orderTag;
            }


            op.LimitPrice = price;

            InstrumentTradeSubscription TS = ttapisubs.TsDictionary[instrument.Key];

            if (!TS.SendOrder(op))
            {
                Console.WriteLine("Send new order failed.  {0}", op.RoutingStatus.Message);
                ttapisubs.Dispose();
            }
            else
            {
                Console.WriteLine("Send new order succeeded.");
            }

            return op.SiteOrderKey;
        }

        public static bool ChangeLimitOrder(string orderKey, TradingTechnologies.TTAPI.Instrument instrument, Subscription ttapisubs,double price, Logger logger)

        {
            InstrumentTradeSubscription TS = ttapisubs.TsDictionary[instrument.Key];
            bool Status = true;

            OrderProfileBase Op = TS.Orders[orderKey].GetOrderProfile();
            Price LimitPrice = Price.FromDouble(instrument.InstrumentDetails, price);

                if (Op.LimitPrice != LimitPrice)
                {
                    Op.LimitPrice = LimitPrice;
                    Op.Action = OrderAction.Change;

                    if (!TS.SendOrder(Op))
                    {
                        logger.Log("Send change order failed.  {0}" + Op.RoutingStatus.Message);
                        Status = false;

                    }
                    else
                    {
                        logger.Log("Send change order succeeded.");
                    }
                }
            
            return Status;
        }

        public static bool CancelLimitOrder(string orderKey, TradingTechnologies.TTAPI.Instrument instrument, Subscription ttapisubs, Logger logger)
        {
            InstrumentTradeSubscription TS = ttapisubs.TsDictionary[instrument.Key];
            bool Status = true;

            OrderProfileBase Op = TS.Orders[orderKey].GetOrderProfile();
            Op.Action = OrderAction.Delete;

                if (!TS.SendOrder(Op))
                {
                    logger.Log("Cancal order failed.  {0}" + Op.RoutingStatus.Message);
                    Status = false;
                }
                else
                {
                    logger.Log("Cancel order succeeded.");
                }

            return Status;

        }

        public static string SendAutospreaderOrder(ttapiUtils.AutoSpreader autoSpreader, 
            int qty, decimal price, string orderTag,Logger logger,int reloadQuantity=0)

        {
            Instrument instrument = autoSpreader.AutoSpreaderInstrument;
            InstrumentDetails instrumentDetails = instrument.InstrumentDetails;

            AutospreaderSyntheticOrderProfile op = new AutospreaderSyntheticOrderProfile(((AutospreaderInstrument)instrument).GetValidGateways()[autoSpreader.GateWay],
                            (AutospreaderInstrument)instrument);

            BuySell Direction = BuySell.Buy;
            Rounding Rounding = Rounding.Down;

            if (qty < 0)
            {
                Direction = BuySell.Sell;
                qty = -qty;
                Rounding = Rounding.Up;
            }

            string OrderKey = op.SiteOrderKey;

            if (reloadQuantity!=0)
            {
                op.SlicerType = SlicerType.Reload;
                op.DisclosedQuantity = Quantity.FromInt(instrument, reloadQuantity);
            }

            op.BuySell = Direction;
            op.OrderQuantity = Quantity.FromInt(instrument, qty);
            op.OrderType = OrderType.Limit;
            op.OrderTag = orderTag;
            op.LimitPrice = Price.FromDouble(instrumentDetails, Convert.ToDouble(price), Rounding);

            if (!autoSpreader.ts.SendOrder(op))
            {
                logger.Log("Send new order failed: " +  op.RoutingStatus.Message);
                
            }
            else
            {
                logger.Log("Send new order succeeded.");
            }

            return OrderKey;

        }

        public static bool ChangeAutospreaderOrder(string orderKey, decimal price, ttapiUtils.AutoSpreader autoSpreader,
            TradingTechnologies.TTAPI.Instrument instrument, Logger logger)
        {
            ASInstrumentTradeSubscription Ts = autoSpreader.ts;
            bool Status = false;
            
            if (Ts.Orders.ContainsKey(orderKey))
            {
                AutospreaderSyntheticOrderProfile Op = Ts.Orders[orderKey].GetOrderProfile() as AutospreaderSyntheticOrderProfile;
                Rounding Rounding = Rounding.Down;

                if (Op.BuySell==BuySell.Sell)
                {
                    Rounding = Rounding.Up;
                }

                Price LimitPrice = Price.FromDouble(instrument.InstrumentDetails, Convert.ToDouble(price), Rounding);

                if (Op.LimitPrice != LimitPrice)
                {
                    Op.LimitPrice = LimitPrice;
                    Op.Action = OrderAction.Change;

                    if (!Ts.SendOrder(Op))
                    {
                        logger.Log("Send change order failed.  {0}" + Op.RoutingStatus.Message);
                    }
                    else
                    {
                        logger.Log("Send change order succeeded.");
                        Status = true;
                    }
                }
            }
            return Status;
        }

        public static bool CancelAutospreaderOrder(string orderKey, ttapiUtils.AutoSpreader autoSpreader, Logger logger)
        {
            ASInstrumentTradeSubscription Ts = autoSpreader.ts;
            bool Status = false;
            if (Ts.Orders.ContainsKey(orderKey))
            {
                AutospreaderSyntheticOrderProfile Op = Ts.Orders[orderKey].GetOrderProfile() as AutospreaderSyntheticOrderProfile;

                Op.Action = OrderAction.Delete;

                if (!Ts.SendOrder(Op))
                {
                    logger.Log("Cancal order failed.  {0}" + Op.RoutingStatus.Message);
                }
                else
                {
                    logger.Log("Cancel order succeeded.");
                    Status = true;
                }
            }

            return Status;
        }
    }
}
