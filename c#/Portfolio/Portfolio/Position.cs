using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Portfolio
{
    public class Position
    {
        public DataTable PositionWithFilledOrders;
        public DataTable PositionWithWorkingOrders;
        public DataTable PositionWithAllOrders;
        public List<string> FullTickerList;

        public Position(List<string> fullTickerList)
        {
            FullTickerList = fullTickerList;
            PositionWithFilledOrders = new DataTable();
            PositionWithFilledOrders.Columns.Add("FullTicker", typeof(string));
            PositionWithFilledOrders.Columns.Add("Qty", typeof(double));

            for (int i = 0; i < FullTickerList.Count; i++)
            {
               PositionWithFilledOrders.Rows.Add();
               PositionWithFilledOrders.Rows[i]["FullTicker"] = FullTickerList[i];
               PositionWithFilledOrders.Rows[i]["Qty"] = 0;
            }

            PositionWithWorkingOrders = PositionWithFilledOrders.Copy();
            PositionWithAllOrders = PositionWithFilledOrders.Copy();
        }

        public void OrderSend(string fullTicker, double qty)
        {
            int tickerIndex = FullTickerList.IndexOf(fullTicker);
            PositionWithWorkingOrders.Rows[tickerIndex]["Qty"] = qty +
                PositionWithWorkingOrders.Rows[tickerIndex].Field<double>("Qty");

            PositionWithAllOrders.Rows[tickerIndex]["Qty"] = qty +
                PositionWithAllOrders.Rows[tickerIndex].Field<double>("Qty");
        }

        public void OrderFill(string fullTicker, double qty)
        {
            int tickerIndex = FullTickerList.IndexOf(fullTicker);
            PositionWithFilledOrders.Rows[tickerIndex]["Qty"] = qty +
                PositionWithFilledOrders.Rows[tickerIndex].Field<double>("Qty");

            PositionWithWorkingOrders.Rows[tickerIndex]["Qty"] = 
                PositionWithWorkingOrders.Rows[tickerIndex].Field<double>("Qty")-qty;
        }




        public double GetFilledPosition4Ticker(string fullTicker)
        {
            return PositionWithFilledOrders.Rows[FullTickerList.IndexOf(fullTicker)].Field<double>("Qty");
        }

        public double GetWorkingPosition4Ticker(string fullTicker)
        {
            return PositionWithWorkingOrders.Rows[FullTickerList.IndexOf(fullTicker)].Field<double>("Qty");
        }

        public double GetTotalPosition4Ticker(string fullTicker)
        {
            return PositionWithAllOrders.Rows[FullTickerList.IndexOf(fullTicker)].Field<double>("Qty");
        }
    }
}
