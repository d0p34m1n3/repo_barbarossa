using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DeltaHedger
{
    public static class Underlying
    {
        public static List<string> FlatCurveTickerClassList = new List<string> { "Index", "FX", "Metal", "Treasury" };

        public static DataTable GetUnderlying2QueryTable(MySqlConnection conn)
        {
            List<string> StrategyList = HedgeStrategies.GetStrategyList2Hedge(conn: conn);
            StrategyList.Add(HedgeStrategies.GetActiveDeltaStrategy(conn: conn));
            DataTable MergedPosition = null;

            for (int i = 0; i < StrategyList.Count; i++)
            {
                if (i == 0)
                    MergedPosition = TA.Strategy.GetNetPosition(alias: StrategyList[i], conn: conn);
                else
                    MergedPosition.Merge(TA.Strategy.GetNetPosition(alias: StrategyList[i], conn: conn));
            }

            DataTable FuturesPosition = MergedPosition.Select("Instrument='F'").CopyToDataTable();
            DataTable OptionsPosition = MergedPosition.Select("Instrument='O'").CopyToDataTable();

            List<string> OptionTickerList = OptionsPosition.AsEnumerable()
                           .Select(r => r.Field<string>("Ticker")).Distinct()
                           .ToList();

            List<string> FuturesTickerList = FuturesPosition.AsEnumerable()
                           .Select(r => r.Field<string>("Ticker")).Distinct()
                           .ToList();

            List<string> UnderlyingTickerList = new List<string>();

            foreach (string item in OptionTickerList)
            {
                UnderlyingTickerList.Add(OptionModels.Utils.GetOptionUnderlying(ticker: item));
            }

            List<string> UniqueList = UnderlyingTickerList.Union(FuturesTickerList).ToList();

            DataTable UnderlyingTable = new DataTable();
            UnderlyingTable.Columns.Add("Ticker", typeof(string));
            UnderlyingTable.Columns.Add("TickerHead", typeof(string));
            UnderlyingTable.Columns.Add("TickerClass", typeof(string));
            UnderlyingTable.Columns.Add("ContINDX", typeof(int));

            for (int i = 0; i < UniqueList.Count; i++)
            {
                DataRow UnderlyingRow = UnderlyingTable.NewRow();
                UnderlyingRow["Ticker"] = UniqueList[i];
                ContractUtilities.ContractSpecs Cs = ContractUtilities.ContractMetaInfo.GetContractSpecs(ticker: UniqueList[i]);
                UnderlyingRow["TickerHead"] = Cs.tickerHead;
                UnderlyingRow["TickerClass"] = Cs.tickerClass;
                UnderlyingRow["ContINDX"] = Cs.contINDX;
                UnderlyingTable.Rows.Add(UnderlyingRow);
            }

            DataRow[] FlatCurveUnderlyings = (from x in UnderlyingTable.AsEnumerable()
                                              where FlatCurveTickerClassList.Any(b => x.Field<string>("TickerClass").Contains(b))
                                              select x).ToArray();

            DataRow[] NonFlatCurveUnderlyings = (from x in UnderlyingTable.AsEnumerable()
                                                 where !FlatCurveTickerClassList.Any(b => x.Field<string>("TickerClass").Contains(b))
                                                 select x).ToArray();

            DataTable FlatCurveQueryTable = new DataTable();
            FlatCurveQueryTable.Columns.Add("Ticker", typeof(string));
            FlatCurveQueryTable.Columns.Add("TickerHead", typeof(string));
            FlatCurveQueryTable.Columns.Add("TickerClass", typeof(string));
            FlatCurveQueryTable.Columns.Add("ContINDX", typeof(int));
            FlatCurveQueryTable.Columns.Add("IsSpreadQ", typeof(bool));

            DataTable NonFlatCurveQueryTable = new DataTable();
            NonFlatCurveQueryTable.Columns.Add("Ticker", typeof(string));
            NonFlatCurveQueryTable.Columns.Add("TickerHead", typeof(string));
            NonFlatCurveQueryTable.Columns.Add("TickerClass", typeof(string));
            NonFlatCurveQueryTable.Columns.Add("ContINDX", typeof(int));
            NonFlatCurveQueryTable.Columns.Add("IsSpreadQ", typeof(bool));

            if (FlatCurveUnderlyings.Count() > 0)
            {
                DataTable FlatCurveUnderlyingTable = FlatCurveUnderlyings.CopyToDataTable();
                ContractUtilities.ContractList liquidContractList = new ContractUtilities.ContractList(FlatCurveUnderlyingTable.AsEnumerable().Select(s => s.Field<string>("TickerHead")).Distinct().ToArray());

                for (int i = 0; i < liquidContractList.dbTickerList.Count; i++)
                {
                    if (!(FlatCurveUnderlyingTable.AsEnumerable().Any(row => liquidContractList.dbTickerList[i] == row.Field<String>("Ticker"))))
                    {
                        DataRow UnderlyingRow = FlatCurveUnderlyingTable.NewRow();
                        UnderlyingRow["Ticker"] = liquidContractList.dbTickerList[i];
                        ContractUtilities.ContractSpecs Cs = ContractUtilities.ContractMetaInfo.GetContractSpecs(ticker: liquidContractList.dbTickerList[i]);
                        UnderlyingRow["TickerHead"] = Cs.tickerHead;
                        UnderlyingRow["TickerClass"] = Cs.tickerClass;
                        UnderlyingRow["ContINDX"] = Cs.contINDX;
                        FlatCurveUnderlyingTable.Rows.Add(UnderlyingRow);
                    }
                }

                for (int i = 0; i < FlatCurveUnderlyingTable.Rows.Count; i++)
                {
                    DataRow Row = FlatCurveQueryTable.NewRow();
                    Row["Ticker"] = FlatCurveUnderlyingTable.Rows[i].Field<string>("Ticker");
                    Row["TickerHead"] = FlatCurveUnderlyingTable.Rows[i].Field<string>("TickerHead");
                    Row["TickerClass"] = FlatCurveUnderlyingTable.Rows[i].Field<string>("TickerClass");
                    Row["ContINDX"] = FlatCurveUnderlyingTable.Rows[i].Field<int>("ContINDX");
                    Row["IsSpreadQ"] = false;
                    FlatCurveQueryTable.Rows.Add(Row);
                }
            }

            if (NonFlatCurveUnderlyings.Count() > 0)
            {
                DataTable SortedNonFlatCurveUnderlyings = NonFlatCurveUnderlyings.AsEnumerable()
                  .OrderBy(r => r.Field<string>("TickerHead"))
                  .ThenBy(r => r.Field<int>("contINDX"))
                  .CopyToDataTable();

                List<string> TickerHeadList = SortedNonFlatCurveUnderlyings.AsEnumerable().Select(s => s.Field<string>("TickerHead")).Distinct().ToList();

                for (int i = 0; i < TickerHeadList.Count(); i++)
                {
                    DataTable TickerHeadPositions = SortedNonFlatCurveUnderlyings.Select("TickerHead='" + TickerHeadList[i] + "'").CopyToDataTable();

                    for (int j = 0; j < TickerHeadPositions.Rows.Count; j++)
                    {
                        DataRow Row = NonFlatCurveQueryTable.NewRow();
                        Row["Ticker"] = TickerHeadPositions.Rows[j].Field<string>("Ticker");
                        Row["TickerHead"] = TickerHeadPositions.Rows[j].Field<string>("TickerHead");
                        Row["TickerClass"] = TickerHeadPositions.Rows[j].Field<string>("TickerClass");
                        Row["ContINDX"] = TickerHeadPositions.Rows[j].Field<int>("ContINDX");
                        Row["IsSpreadQ"] = false;
                        NonFlatCurveQueryTable.Rows.Add(Row);
                    }

                    for (int j = 0; j < TickerHeadPositions.Rows.Count - 1; j++)
                    {

                        for (int k = j + 1; k < TickerHeadPositions.Rows.Count; k++)
                        {
                            DataRow Row2 = NonFlatCurveQueryTable.NewRow();
                            Row2["Ticker"] = TickerHeadPositions.Rows[j].Field<string>("Ticker") + "-" + TickerHeadPositions.Rows[k].Field<string>("Ticker");
                            Row2["TickerHead"] = TickerHeadPositions.Rows[j].Field<string>("TickerHead");
                            Row2["TickerClass"] = TickerHeadPositions.Rows[j].Field<string>("TickerClass");
                            Row2["ContINDX"] = TickerHeadPositions.Rows[j].Field<int>("ContINDX");
                            Row2["IsSpreadQ"] = true;
                            NonFlatCurveQueryTable.Rows.Add(Row2);
                        }
                    }

                }
            }
            FlatCurveQueryTable.Merge(NonFlatCurveQueryTable);
            return FlatCurveQueryTable;
        }
    }
}
