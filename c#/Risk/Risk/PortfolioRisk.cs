using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using ContractUtilities;
using CalendarUtilities;
using IOUtilities;
using TA;
using Shared;

namespace Risk
{
    public static class PorfolioRisk
    {
        public static double GetPortfolioRiskFromCovMatrix(DataTable positionTable, DataTable covMatrix)
        {
            double portfolioStd = 0;
            DataRow[] positionRows = positionTable.Select("Qty<>0");

            if (positionRows.Length > 0)
            {
                for (int i = 0; i < positionRows.Length; i++)
                {
                    string tickerI = positionRows[i].Field<string>("FullTicker");
                    string tickerheadI = ContractMetaInfo.GetContractSpecs(tickerI).tickerHead;
                    DataRow[] covRows = covMatrix.Select("index='" + tickerheadI + "'");

                    portfolioStd = portfolioStd + covRows[0].Field<double>(tickerheadI) * Math.Pow(positionRows[i].Field<double>("Qty"), 2);

                    for (int j = i + 1; j < positionRows.Length; j++)
                    {
                        string tickerJ = positionRows[j].Field<string>("FullTicker");
                        string tickerheadJ = ContractMetaInfo.GetContractSpecs(tickerJ).tickerHead;
                        portfolioStd = portfolioStd + 2 * covRows[0].Field<double>(tickerheadJ) *
                        positionRows[i].Field<double>("Qty") * positionRows[j].Field<double>("Qty");
                    }
                }
            }
            return Math.Sqrt(portfolioStd);
        }

        public static double GetStd4Ticker(string fullTicker, DataTable covMatrix)
        {
            string TickerHead = ContractMetaInfo.GetContractSpecs(fullTicker).tickerHead;
            DataRow[] covRows = covMatrix.Select("index='" + TickerHead + "'");
            return Math.Sqrt(covRows[0].Field<double>(TickerHead));
        }

        public static double GetChangeInRiskAfterTickerInclusion(DataTable positionTable, DataTable covMatrix,string ticker2Include,double qty)
        {
            string TickerHead = ContractMetaInfo.GetContractSpecs(ticker2Include).tickerHead;
            DataRow[] covRows = covMatrix.Select("index='" + TickerHead + "'");
            DataRow[] positionRows = positionTable.Select("Qty<>0");
            double portfolioVarChange = covRows[0].Field<double>(TickerHead) * Math.Pow(qty, 2);

            for (int i = 0; i < positionRows.Length; i++)
            {
                string TickerI = positionRows[i].Field<string>("FullTicker");
                string TickerHeadI = ContractMetaInfo.GetContractSpecs(TickerI).tickerHead;
                portfolioVarChange = portfolioVarChange + 2 * covRows[0].Field<double>(TickerHeadI) *
                positionRows[i].Field<double>("Qty") * qty;
            }
            return portfolioVarChange;
        }

        public static DataTable LoadCovMatrix()
        {
            DateTime FolderDate;
            string DirectoyRoot = DirectoryNames.GetDirectoryName("overnightCandlestick");
            string FullFolderName;
            DataTable CovMatrix = null;

            for (int i = 1; i < 6; i++)
            {
                FolderDate = BusinessDays.GetBusinessDayShifted(-i);
                FullFolderName = DirectoyRoot + DirectoryNames.GetDirectoryExtension(FolderDate);

                double CovDataIntegrity = Convert.ToDouble(FromTxt2List.LoadFile(FullFolderName + "/covDataIntegrity.txt")[0]);

                if (i > 1)
                {
                    Console.WriteLine("Most recent covFile not available or lower quality! ");
                }

                if (CovDataIntegrity > 80)
                {
                    CovMatrix = IOUtilities.ExcelDataReader.LoadFile(FullFolderName + "/cov_matrix.xlsx").Tables["cov_matrix"];
                    break;
                }
            }
            return CovMatrix;
        }




    }
}
