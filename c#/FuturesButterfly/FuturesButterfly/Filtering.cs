using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FuturesButterfly
{
    public static class Filtering
    {
        public static int MinTrDte = 45;
        public static double MinNewPosition = 1000;
        public static Dictionary<string, double> PositionSizeDictionary = Risk.PositionSizing.GetStrategyTargetSize();
        public static double ButterflyFirstBetSize = PositionSizeDictionary["ButterflyFirstBetSize"];
        public static double ButterflyFinalBetSize = PositionSizeDictionary["ButterflyFinalBetSize"];
        private static int ContIndx1YearOut;

        public static DataTable GetFilteredNewTrades(DataTable butterfliesSheet,DataTable riskAcrossTickerhead,Dictionary<string,double>riskAcrossTheme)
        {
            DataTable ButterfliesSheet = butterfliesSheet.Select("trDte1>=" + MinTrDte).CopyToDataTable();
            List<string> TickerHeadNewList = DataAnalysis.DataTableFunctions.GetColumnAsList<string>(dataTableInput: ButterfliesSheet, columnName: "tickerHead", uniqueQ: true);

            DataTable ButterfliesSheetFiltered = new DataTable();
            ButterfliesSheetFiltered = ButterfliesSheet.Clone();

            DateTime CurrentDate = DateTime.Now.Date;
            ContIndx1YearOut = 100 * (CurrentDate.Year + 1) + CurrentDate.Month;

            for (int i = 0; i < ButterfliesSheet.Rows.Count; i++)
            {
                DataRow SelectedRow = ButterfliesSheet.Rows[i];

                List<string> TickerList = new List<string> {SelectedRow.Field<string>("Ticker1"), SelectedRow.Field<string>("Ticker2"), SelectedRow.Field<string>("Ticker3")};
                double Direction;

                if (SelectedRow.Field<double>("z1") < 0)
                {
                    Direction = 1; 
                }
                else
                {
                    Direction = -1;
                }

                List<string> ThemeList = RiskManager.GetThemeList4Position(tickerList: TickerList, direction: Direction);

                if (ThemeList.Count>0)
                {
                    List<double> RelatedThemeDownsideList = new List<double>();
                    for (int j = 0; j < ThemeList.Count; j++)
			{
                RelatedThemeDownsideList.Add(ButterflyFinalBetSize*RiskManager.ThemeMultipliers[ThemeList[j]]-Math.Abs(riskAcrossTheme[ThemeList[j]]));
                SelectedRow["SlackFromTheme"] = RelatedThemeDownsideList.Min();
			}       
                }
                else
                {
                    SelectedRow["SlackFromTheme"] = ButterflyFinalBetSize; 
                }
                    

            }

            for (int i = 0; i < TickerHeadNewList.Count; i++)
            {

                DataTable Selected4TickerHeadTable = ButterfliesSheet.Select("tickerHead='" + TickerHeadNewList[i] + "'").CopyToDataTable();

                DataRow[] Long4TickerHead = Selected4TickerHeadTable.Select("z1<0");
                DataRow[] Short4TickerHead = Selected4TickerHeadTable.Select("z1>0");

                if (Long4TickerHead.Count() > 0)
                {
                    DataRow[] ExistingLongRows = riskAcrossTickerhead.Select("TickerHead='" + TickerHeadNewList[i] + "' and Direction=1");

                    if (ExistingLongRows.Count() == 0)
                    {
                        for (int j = 0; j < Long4TickerHead.Count(); j++)
                        {
                            if (Math.Min(ButterflyFinalBetSize,Long4TickerHead[j].Field<double>("SlackFromTheme")) > MinNewPosition)
                            {
                                Long4TickerHead[j]["Size"] = Math.Min(ButterflyFinalBetSize, Long4TickerHead[j].Field<double>("SlackFromTheme"));
                                ButterfliesSheetFiltered.ImportRow(Long4TickerHead[j]);
                            }
                        }
                    }

                    else if ((ButterflyFinalBetSize - Math.Abs(ExistingLongRows[0].Field<double>("DownSide")) > MinNewPosition))

                        for (int j = 0; j < Long4TickerHead.Count(); j++)
                        {
                            if (Math.Min(ButterflyFinalBetSize, Long4TickerHead[j].Field<double>("SlackFromTheme")) > MinNewPosition)
                            {
                                Long4TickerHead[j]["Size"] = Math.Min(ButterflyFinalBetSize - Math.Abs(ExistingLongRows[0].Field<double>("DownSide")),
                                                                      Math.Min(ButterflyFinalBetSize, Long4TickerHead[j].Field<double>("SlackFromTheme")));
                                ButterfliesSheetFiltered.ImportRow(Long4TickerHead[j]);
                            }

                        }
                }

                if (Short4TickerHead.Count() > 0)
                {
                    DataRow[] ExistingShortRows = riskAcrossTickerhead.Select("TickerHead='" + TickerHeadNewList[i] + "' and Direction=-1");


                    if (ExistingShortRows.Count() == 0)
                    {
                        for (int j = 0; j < Short4TickerHead.Count(); j++)
                        {
                            if (Short4TickerHead[j].Field<double>("z1") >= 1.2)
                            {
                                if (Math.Min(ButterflyFinalBetSize, Short4TickerHead[j].Field<double>("SlackFromTheme")) > MinNewPosition)
                                {
                                    Short4TickerHead[j]["Size"] = Math.Min(ButterflyFinalBetSize, Short4TickerHead[j].Field<double>("SlackFromTheme"));
                                }

                            }
                            else if (Short4TickerHead[j].Field<double>("z1") >= 0.6)
                            {
                                if (Math.Min(ButterflyFirstBetSize, Short4TickerHead[j].Field<double>("SlackFromTheme"))>MinNewPosition)
                                {
                                    Short4TickerHead[j]["Size"] = Math.Min(ButterflyFirstBetSize, Short4TickerHead[j].Field<double>("SlackFromTheme"));
                                }
                            }

                            if (Short4TickerHead[j].Field<double>("Size") > 0)
                            {
                                ButterfliesSheetFiltered.ImportRow(Short4TickerHead[j]);
                            }

                        }
                    }

                    else
                    {
                        for (int j = 0; j < Short4TickerHead.Count(); j++)
                        {

                            if ((Short4TickerHead[j].Field<double>("z1") >= 1.2) &&
                                (ButterflyFinalBetSize - Math.Abs(ExistingShortRows[0].Field<double>("DownSide")) > MinNewPosition) &&
                                (Math.Min(ButterflyFinalBetSize, Short4TickerHead[j].Field<double>("SlackFromTheme")) > MinNewPosition)
                                )
                            {
                                Short4TickerHead[j]["Size"] = Math.Min(ButterflyFinalBetSize - Math.Abs(ExistingShortRows[0].Field<double>("DownSide")),
                                                                       Math.Min(ButterflyFinalBetSize, Short4TickerHead[j].Field<double>("SlackFromTheme")));
                            }
                            else if ((Short4TickerHead[j].Field<double>("z1") >= 0.6) &&
                               (ButterflyFirstBetSize - Math.Abs(ExistingShortRows[0].Field<double>("DownSide")) > MinNewPosition) &&
                                (Math.Min(ButterflyFirstBetSize, Short4TickerHead[j].Field<double>("SlackFromTheme")) > MinNewPosition))
                            {
                                Short4TickerHead[j]["Size"] = Math.Min(ButterflyFirstBetSize - Math.Abs(ExistingShortRows[0].Field<double>("DownSide")),
                                                                       Math.Min(ButterflyFirstBetSize, Short4TickerHead[j].Field<double>("SlackFromTheme")));
                            }

                            if (Short4TickerHead[j].Field<double>("Size") > 0)
                            {
                                ButterfliesSheetFiltered.ImportRow(Short4TickerHead[j]);
                            }

                        }
                    }
                }
            }
            return ButterfliesSheetFiltered;
        }
    }
}
