using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ContractUtilities;
using MySql.Data;
using MySql.Data.MySqlClient;
using System.Data;

namespace OptionModels
{
    public static class Utils
    {
        public static string GetOptionUnderlying(string ticker)
        {
            ContractUtilities.ContractSpecs ContSpecsOutput = ContractUtilities.ContractMetaInfo.GetContractSpecs(ticker);
            string TickerHead = ContSpecsOutput.tickerHead;
            int TickerYear = ContSpecsOutput.tickerYear;
            int TickerMonthNum = ContSpecsOutput.tickerMonthNum;
            int UnderlyingMonthNum;
            int UnderlyingYear;

            if (TickerHead=="E0")
            {
                TickerHead="ED";
                TickerYear = TickerYear+1;
            }
            else if (TickerHead=="E2")
            {
                TickerHead = "ED";
                TickerYear = TickerYear+2;
            }
            else if (TickerHead == "E3")
            {
                TickerHead = "ED";
                TickerYear = TickerYear + 3;
            }
            else if (TickerHead == "E4")
            {
                TickerHead = "ED";
                TickerYear = TickerYear + 4;
            }
            else if (TickerHead == "E5")
            {
                TickerHead = "ED";
                TickerYear = TickerYear + 5;
            }

            List<string> FuturesContractMonthsOutput = ContractUtilities.ContractMetaInfo.FuturesContractMonths[TickerHead];

            List<int> ContractMonthNumbers = new List<int>();

            foreach (string item in FuturesContractMonthsOutput)
            {
                ContractMonthNumbers.Add(ContractUtilities.ContractMetaInfo.fullLetterMonthList.FindIndex(x => x == item) + 1);
            }

            List<int> LeadingMonths = ContractMonthNumbers.Where(x => x >= TickerMonthNum).ToList();

            if (LeadingMonths.Count>0)
            {
                UnderlyingMonthNum = LeadingMonths[0];
                UnderlyingYear = TickerYear;
            }
            else
            {
                UnderlyingMonthNum = ContractMonthNumbers[0];
                UnderlyingYear = TickerYear + 1;
            }

            return TickerHead + ContractUtilities.ContractMetaInfo.fullLetterMonthList[UnderlyingMonthNum - 1] + UnderlyingYear.ToString();
        }

        public static AugmentedGreeks OptionModelWrapper(string modelName,string ticker, string optionType, double strike,MySqlConnection conn,
            DateTime calculationDate,double optionPrice=double.NaN,double impliedVol=double.NaN,double underlyingPrice=double.NaN,DateTime? interestRateDate = null)
        {
            if (interestRateDate==null)
            {
                interestRateDate = calculationDate;
            }

            AugmentedGreeks AugmentedGreeksOutput = new AugmentedGreeks();

            string UnderlyingTicker = GetOptionUnderlying(ticker);

            
            if (double.IsNaN(underlyingPrice))
            {
                DataTable UnderlyingDataFrame = GetPrice.GetFuturesPrice.getFuturesPrice4Ticker(ticker: UnderlyingTicker,
               dateTimeFrom: calculationDate, dateTimeTo: calculationDate, conn: conn);
                if (UnderlyingDataFrame.Rows.Count == 0)
                {
                    return AugmentedGreeksOutput;
                }
                underlyingPrice = (double)(UnderlyingDataFrame.Rows[0].Field<decimal>("close_price"));
            }

            ContractUtilities.ContractSpecs ContractSpecsOut = ContractUtilities.ContractMetaInfo.GetContractSpecs(ticker: UnderlyingTicker);
            string TickerHead = ContractSpecsOut.tickerHead;
            double ContractMultiplier = ContractUtilities.ContractMetaInfo.ContractMultiplier[TickerHead];
            string ExerciseType = ContractUtilities.ContractMetaInfo.GetOptionExerciseType(TickerHead);

            DateTime ExpirationDate = (DateTime)Expiration.getExpirationFromDB(ticker: ticker, instrument: "options", conn: conn);

            double InterestRate = InterestCurve.RateFromStir.GetSimpleRate(asOfDate: (DateTime)interestRateDate, 
                dateFrom: (DateTime)interestRateDate,
                dateTo: ExpirationDate, tickerHead: "ED", conn: conn);

            
            if (Double.IsNaN(InterestRate))
            {
                return AugmentedGreeksOutput;
            }
            else
            {
                Greeks GreeksOutput = QuantlibOptionModels.GetOptionOnFutureGreeks(underlyingPrice: underlyingPrice, strike: strike, riskFreeRate: InterestRate,
                    expirationDate: ExpirationDate, calculationDate: calculationDate, optionType: optionType, exerciseType: ExerciseType, optionPrice: optionPrice,impliedVol:impliedVol);
                AugmentedGreeksOutput.OptionPrice = GreeksOutput.OptionPrice;
                AugmentedGreeksOutput.ImpliedVol = 100*GreeksOutput.ImpliedVol;
                AugmentedGreeksOutput.Delta = GreeksOutput.Delta;
                AugmentedGreeksOutput.Vega = GreeksOutput.Vega;
                AugmentedGreeksOutput.Theta = GreeksOutput.Theta;
                AugmentedGreeksOutput.CalDte = GreeksOutput.CalDte;
                AugmentedGreeksOutput.Gamma = GreeksOutput.Gamma;
                AugmentedGreeksOutput.DollarVega = GreeksOutput.Vega*ContractMultiplier/100;
                AugmentedGreeksOutput.DollarTheta = GreeksOutput.Theta*ContractMultiplier;
                AugmentedGreeksOutput.DollarGamma = GreeksOutput.Gamma*ContractMultiplier;
                AugmentedGreeksOutput.InterestRate = InterestRate;

            }

            return AugmentedGreeksOutput;


        }
    }
}
