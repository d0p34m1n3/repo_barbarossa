using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ContractUtilities;

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
    }
}
