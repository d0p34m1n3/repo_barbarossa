using MySql.Data.MySqlClient;
using StrategyUtilities;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FuturesButterfly
{
    public static class PositionManager
    {
        public static PositionManagerOutput GetFuturesButterflyPosition(string alias, DateTime asOfDate, MySqlConnection conn)
        {
            DataTable StrategyPosition = TA.Strategy.GetNetPosition(alias: alias, conn: conn, asOfDate: asOfDate);
            StrategyUtilities.PositionManagerOutput PositionManagerOut = new StrategyUtilities.PositionManagerOutput();

            if (StrategyPosition.Rows.Count == 0)
            {
                return PositionManagerOut;
            }

            StrategyPosition.Columns.Add("TickerHead", typeof(string));
            StrategyPosition.Columns.Add("SortIndex", typeof(int));

            foreach (DataRow Row in StrategyPosition.Rows)
            {
                ContractUtilities.ContractSpecs Cs = ContractUtilities.ContractMetaInfo.GetContractSpecs(ticker: Row.Field<string>("Ticker"));
                Row["TickerHead"] = Cs.tickerHead;
                Row["SortIndex"] = Cs.contINDX;
            }

            StrategyPosition = DataAnalysis.DataTableFunctions.Sort(dataTableInput: StrategyPosition, columnList: new string[] { "SortIndex" });
            bool CorrectPositionQ = false;

            if ((StrategyPosition.Rows.Count == 3) &&
                (Math.Sign(StrategyPosition.Rows[0].Field<double>("Qty"))) == Math.Sign(StrategyPosition.Rows[2].Field<double>("Qty")) &&
                (StrategyPosition.AsEnumerable().Sum(x => x.Field<double>("Qty")) == 0))
            {
                CorrectPositionQ = true;
            }

            PositionManagerOut.SortedPosition = StrategyPosition;
            PositionManagerOut.Scale = StrategyPosition.Rows[0].Field<double>("Qty");
            PositionManagerOut.CorrectPositionQ = CorrectPositionQ;
            return PositionManagerOut;
        }
    }
}
