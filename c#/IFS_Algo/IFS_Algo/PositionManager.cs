using MySql.Data.MySqlClient;
using StrategyUtilities;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IFS_Algo
{
    
    public static class PositionManager
    {
        public static PositionManagerOutput GetIfsPosition(string alias, DateTime asOfDate,MySqlConnection conn)
        {
            DataTable StrategyPosition = TA.Strategy.GetNetPosition(alias: alias, conn: conn, asOfDate: asOfDate);
            StrategyUtilities.PositionManagerOutput PositionManagerOut = new StrategyUtilities.PositionManagerOutput();

            if (StrategyPosition.Rows.Count==0)
            {
                return PositionManagerOut;
            }

            StrategyPosition.Columns.Add("TickerHead", typeof(string));
            StrategyPosition.Columns.Add("SortIndex", typeof(int));

            foreach (DataRow Row in StrategyPosition.Rows)
            {
                Row["TickerHead"] = ContractUtilities.ContractMetaInfo.GetContractSpecs(ticker: Row.Field<string>("Ticker")).tickerHead;
            }

            List<string> TickerHeadList = DataAnalysis.DataTableFunctions.GetColumnAsList<string>(dataTableInput: StrategyPosition, columnName: "TickerHead");
            List<string> TickerList = new List<string>();
            List<double> QtyList = new List<double>();
            List<double> ScaleList = new List<double>();
            bool CorrectPositionQ = false;

            Dictionary<string, List<decimal>> WeightDictionary = StrategyUtilities.SpreadDefinitions.WeightDictionary;

            foreach (KeyValuePair<string, List<decimal>> entry in WeightDictionary)
            {
                List<string> TickerHeadListFromDefinition = entry.Key.Split('_').ToList();

                if (new HashSet<string>(TickerHeadListFromDefinition).SetEquals(TickerHeadList))
                {
                    CorrectPositionQ = true;
                    for (int i = 0; i < TickerHeadList.Count; i++)
                    {
                        int RowIndex = TickerHeadListFromDefinition.IndexOf(TickerHeadList[i]);
                        StrategyPosition.Rows[i]["SortIndex"] = RowIndex;

                        ScaleList.Add(StrategyPosition.Rows[i].Field<double>("Qty")/(double)entry.Value[RowIndex]);
                    }

                    for (int i = 1; i < ScaleList.Count; i++)
                    {
                        if (Math.Abs(ScaleList[i] - ScaleList[1]) > 0.1)
                        {
                            CorrectPositionQ = false;
                        }
                    }
                    break;
                }
            }

            StrategyPosition = DataAnalysis.DataTableFunctions.Sort(dataTableInput: StrategyPosition, columnList:new string[] { "SortIndex" });

            PositionManagerOut.SortedPosition = StrategyPosition;
            PositionManagerOut.Scale = ScaleList[0];
            PositionManagerOut.CorrectPositionQ = CorrectPositionQ;
            return PositionManagerOut;

        }
    }
}
