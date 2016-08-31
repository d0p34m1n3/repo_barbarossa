using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TradingTechnologies.TTAPI;
using TradingTechnologies.TTAPI.Tradebook;

namespace ttapiUtils
{
    public static class Trade
    {
        public static void SendLimitOrder(TradingTechnologies.TTAPI.Instrument instrument,
            TradingTechnologies.TTAPI.Price price,int qty,Subscription ttapisubs)

        {
            BuySell Direction = BuySell.Buy;
            string AccountName = "H1KOC";

            if (qty<0)
            {
                Direction = BuySell.Sell;
                qty = -qty;
            }

            string tickerDB = TA.TickerConverters.ConvertFromTTAPIFields2DB(instrument.Product.ToString(), instrument.Name.ToString());
            string TickerHead = ContractUtilities.ContractMetaInfo.GetContractSpecs(tickerDB).tickerHead;
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
        }
    }
}
