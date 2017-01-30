using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FuturesButterfly
{
    public static class RiskManager
    {
        public static List<string> CattleTickerHeadGroup = new List<string>{"LC","FC"};
        public static List<string> SoyTickerHeadGroup = new List<string>{"S","SM","BO"};
        public static List<string> WtiCrudeOilTickerHeadGroup = new List<string>{"CL","HO","RB"};
        public static List<string> CrudeOilTickerHeadGroup = new List<string> { "CL", "HO", "RB" ,"B"};
        public static List<string> WheatTickerHeadGroup = new List<string>{"W","KW"};

        public static Dictionary<string, double> ThemeMultipliers = new Dictionary<string, double>(){{"Cattle_ShortSpringAgainstSummer",1.2},
                                                                                   {"Cattle_LongSpringAgainstSummer",1.2},
                                                                                   {"Soy_Short",1.2},
                                                                                   {"Soy_Long",1.2},
                                                                                   {"Wheat_Short",1.2},
                                                                                   {"Wheat_Long",1.2},
                                                                                   {"WtiCrudeOil_Long",1.2},
                                                                                   {"WtiCrudeOil_Short",1.2},
                                                                                   {"CrudeOilShortDated_Long",1.2},
                                                                                   {"CrudeOilShortDated_Short",1.2},
                                                                                   {"RB_LongSummer",0.6},
                                                                                   {"RB_ShortSummer",0.6},
                                                                                   {"NG_ShortWinter",1.2},
                                                                                   {"NG_LongWinter",1.2}
                                                                                  };


        private static int ContIndx1YearOut;

        public static List<string> GetThemeList4Position(List<string> tickerList,double direction)
        {
            List<string> ThemeList = new List<string>();
            ContractUtilities.ContractSpecs Cs1 = ContractUtilities.ContractMetaInfo.GetContractSpecs(tickerList[0]);
                    ContractUtilities.ContractSpecs Cs2 = ContractUtilities.ContractMetaInfo.GetContractSpecs(tickerList[1]);
                    ContractUtilities.ContractSpecs Cs3 = ContractUtilities.ContractMetaInfo.GetContractSpecs(tickerList[2]);

            string TickerHead = Cs1.tickerHead;

            if ((TickerHead == "LC") && (Cs1.tickerMonthNum < 8) && (Cs2.tickerMonthNum == 8))
                    {
                        if (direction < 0)
                        {
                            ThemeList.Add("Cattle_ShortSpringAgainstSummer");
                        }
                        else if (direction > 0)
                        {
                            ThemeList.Add("Cattle_LongSpringAgainstSummer");
                        }
                    }

            if ((TickerHead == "LC") && (Cs2.tickerMonthNum == 4))
            {
                if (direction > 0)
                {
                    ThemeList.Add("Cattle_ShortSpringAgainstSummer");
                }
                else if (direction < 0)
                {
                    ThemeList.Add("Cattle_LongSpringAgainstSummer");
                }
            }

                    if ((TickerHead == "FC") && (Cs2.tickerMonthNum == 3))
                    {
                        if (direction < 0)
                        {
                            ThemeList.Add("Cattle_ShortSpringAgainstSummer");
                        }
                        else if (direction > 0)
                        {
                            ThemeList.Add("Cattle_LongSpringAgainstSummer");
                        }
                    }

                    if (SoyTickerHeadGroup.Contains(TickerHead))
                    {
                        if (direction < 0)
                        {
                            ThemeList.Add("Soy_Short");
                        }
                        else if (direction > 0)
                        {
                            ThemeList.Add("Soy_Long");
                        }
                    }

                    if (WheatTickerHeadGroup.Contains(TickerHead))
                    {
                        if (direction < 0)
                        {
                            ThemeList.Add("Wheat_Short");
                        }
                        else if (direction > 0)
                        {
                            ThemeList.Add("Wheat_Long");
                        }
                    }

                    if ((WtiCrudeOilTickerHeadGroup.Contains(TickerHead)) && (direction > 0))
                    {
                        ThemeList.Add("WtiCrudeOil_Long");
                    }

                    if ((WtiCrudeOilTickerHeadGroup.Contains(TickerHead)) && (direction < 0))
                    {
                        ThemeList.Add("WtiCrudeOil_Short");
                    }

                    if ((CrudeOilTickerHeadGroup.Contains(TickerHead)) && (Cs1.contINDX < ContIndx1YearOut) && (direction > 0))
                    {
                        ThemeList.Add("CrudeOilShortDated_Long");
                    }

                    if ((CrudeOilTickerHeadGroup.Contains(TickerHead)) && (Cs1.contINDX < ContIndx1YearOut) && (direction < 0))
                    {
                        ThemeList.Add("CrudeOilShortDated_Short");
                    }

                    if ((TickerHead == "RB") && ((Cs2.tickerMonthNum == 5) || (Cs2.tickerMonthNum == 4)))
                    {
                        if (direction < 0)
                        {
                            ThemeList.Add("RB_LongSummer");
                        }
                        else if (direction > 0)
                        {
                            ThemeList.Add("RB_ShortSummer");
                        }
                    }

                    if ((TickerHead == "RB") && (Cs2.tickerMonthNum == 10))
                    {
                        if (direction < 0)
                        {
                            ThemeList.Add("RB_ShortSummer");
                        }
                        else if (direction > 0)
                        {
                            ThemeList.Add("RB_LongSummer");
                        }
                    }

                    if ((TickerHead == "NG") && (Cs2.tickerMonthNum == 7))
                    {
                        if (direction > 0)
                        {
                            ThemeList.Add("NG_ShortWinter");
                        }
                        else if (direction < 0)
                        {
                            ThemeList.Add("NG_LongWinter");
                        }
                    }

                    if ((TickerHead == "NG") && (Cs2.tickerMonthNum == 11))
                    {
                        if (direction < 0)
                        {
                            ThemeList.Add("NG_ShortWinter");
                        }
                        else if (direction > 0)
                        {
                            ThemeList.Add("NG_LongWinter");
                        }
                    }
            return ThemeList;
                }

        

        public static Dictionary<string,double> GetRiskAcrossTheme(DataTable dataTableInput,Dictionary<string,DataTable> strategyPositionDictionary)

        {

            DateTime CurrentDate = DateTime.Now.Date;
            ContIndx1YearOut = 100 * (CurrentDate.Year + 1) + CurrentDate.Month;
            Dictionary<string,double> RiskAcrossTheme = new Dictionary<string,double>(){{"Cattle_ShortSpringAgainstSummer",0},
                                                                                   {"Cattle_LongSpringAgainstSummer",0},
                                                                                   {"Soy_Short",0},
                                                                                   {"Soy_Long",0},
                                                                                   {"Wheat_Short",0},
                                                                                   {"Wheat_Long",0},
                                                                                   {"CrudeOil_Long",0},
                                                                                   {"CrudeOil_Short",0},
                                                                                   {"CrudeOilShortDated_Long",0},
                                                                                   {"CrudeOilShortDated_Short",0},
                                                                                   {"RB_LongSummer",0},
                                                                                   {"RB_ShortSummer",0},
                                                                                   {"NG_ShortWinter",0},
                                                                                   {"NG_LongWinter",0}
                                                                                  };

            for (int i = 0; i < dataTableInput.Rows.Count; i++)
            {

                if (strategyPositionDictionary.ContainsKey(dataTableInput.Rows[i].Field<string>("Alias")))
                {

                    DataTable PositionTable = strategyPositionDictionary[dataTableInput.Rows[i].Field<string>("Alias")];

                    List<string> TickerList = DataAnalysis.DataTableFunctions.GetColumnAsList<string>(dataTableInput: PositionTable, columnName: "Ticker");
                    List<string> ThemeList = GetThemeList4Position(tickerList: TickerList, direction: PositionTable.Rows[0].Field<double>("Qty"));

                    for (int j = 0; j < ThemeList.Count(); j++)
                    {
                        if (RiskAcrossTheme.ContainsKey(ThemeList[j]))
                        {
                            RiskAcrossTheme[ThemeList[j]] = RiskAcrossTheme[ThemeList[j]] + dataTableInput.Rows[i].Field<double>("DownsideAdjusted");
                        }
                        else
                        {
                            RiskAcrossTheme.Add(ThemeList[j], dataTableInput.Rows[i].Field<double>("DownsideAdjusted"));
                        }
                    }
                }
            }
        
            return RiskAcrossTheme;
        }

        public static DataTable CalculateAdjustedDownside(DataTable dataTableInput, DataTable newTradesTable, Dictionary<string, DataTable> strategyPositionDictionary)
        {
            dataTableInput.Columns.Add("DownsideAdjusted", typeof(double));


            for (int i = 0; i < dataTableInput.Rows.Count; i++)
            {

                DataRow StrategyRow = dataTableInput.Rows[i];
                StrategyRow["DownsideAdjusted"] = StrategyRow.Field<double>("Downside");
                if (strategyPositionDictionary.ContainsKey(StrategyRow.Field<string>("Alias")))
                {
                    DataTable PositionTable = strategyPositionDictionary[StrategyRow.Field<string>("Alias")];
                    DataRow[] MatchingNewTrade;

                    if (PositionTable.Rows[0].Field<double>("Qty") > 0)
                    {
                        MatchingNewTrade = newTradesTable.Select("ticker1='" + PositionTable.Rows[0].Field<string>("Ticker") + "' and " +
                                          "ticker2='" + PositionTable.Rows[1].Field<string>("Ticker") + "' and " +
                                          "ticker3='" + PositionTable.Rows[2].Field<string>("Ticker") + "' and z1<0");
                    }
                    else
                    {
                        MatchingNewTrade = newTradesTable.Select("ticker1='" + PositionTable.Rows[0].Field<string>("Ticker") + "' and " +
                                          "ticker2='" + PositionTable.Rows[1].Field<string>("Ticker") + "' and " +
                                          "ticker3='" + PositionTable.Rows[2].Field<string>("Ticker") + "' and z1>0");
                    }

                    if (MatchingNewTrade.Length == 0)
                    {
                        StrategyRow["DownsideAdjusted"] = 1.5 * StrategyRow.Field<double>("DownsideAdjusted");
                    }


                }

                if (StrategyRow.Field<double>("ShortTrDte") < 65)
                {
                    StrategyRow["DownsideAdjusted"] = Math.Round(StrategyRow.Field<double>("DownsideAdjusted") *
                        (65 - StrategyRow.Field<double>("ShortTrDte") + 30) / 30);
                }
            }

            return dataTableInput;
        }

        public static DataTable AggregateRiskAcrossTickerHead(DataTable dataTableInput)
        {
            List<string> TickerHeadList = DataAnalysis.DataTableFunctions.GetColumnAsList<string>(dataTableInput: dataTableInput, columnName: "TickerHead", uniqueQ: true);

            DataTable AggregateTable = new DataTable();
            AggregateTable.Columns.Add("TickerHead", typeof(string));
            AggregateTable.Columns.Add("Direction", typeof(int));
            AggregateTable.Columns.Add("Downside", typeof(double));
           
            for (int i = 0; i < TickerHeadList.Count; i++)
            {
                double LongExposure = 0;
                double ShortExposure = 0;
                DataRow[] SelectStrategyRows = dataTableInput.Select("TickerHead='" + TickerHeadList[i] + "'");

                for (int j = 0; j < SelectStrategyRows.Length; j++)
                {
                        
                
                    if (SelectStrategyRows[j].Field<double>("Z1Initial") > 0)
                    {
                        ShortExposure = ShortExposure + SelectStrategyRows[j].Field<double>("DownsideAdjusted");
                    }
                    else if (SelectStrategyRows[j].Field<double>("Z1Initial") < 0)
                    {
                        LongExposure = LongExposure + SelectStrategyRows[j].Field<double>("DownsideAdjusted");
                    }
                }
             

                if (LongExposure != 0)
                {
                    DataRow AggregateRow = AggregateTable.NewRow();
                    AggregateRow["TickerHead"] = TickerHeadList[i];
                    AggregateRow["Direction"] = 1;
                    AggregateRow["Downside"] = LongExposure;
                    AggregateTable.Rows.Add(AggregateRow);
                }

                if (ShortExposure != 0)
                {
                    DataRow AggregateRow = AggregateTable.NewRow();
                    AggregateRow["TickerHead"] = TickerHeadList[i];
                    AggregateRow["Direction"] = -1;
                    AggregateRow["Downside"] = ShortExposure;
                    AggregateTable.Rows.Add(AggregateRow);
                }
            }


            return AggregateTable;
        }
    }
}
