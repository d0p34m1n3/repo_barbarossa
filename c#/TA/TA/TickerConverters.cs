using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ContractUtilities;

namespace TA
{
    public class ttapiTicker
    {
        public string productName { set; get; }
        public string instrumentName { set; get;}
        public string marketKey { set; get; }
        public string productType { set; get; }
        public string SeriesKey { set; get; }
    }

    public static class TickerConverters
    {
        public static Dictionary<string, string> TickerMonthFromTT2DB =
            new Dictionary<string, string>()
            {
            {"Jan","F"},{"Feb","G"},{"Mar","H"},{"Apr","J"},
            {"May","K"},{"Jun","M"},{"Jul","N"},{"Aug","Q"},
            {"Sep","U"},{"Oct","V"},{"Nov","X"},{"Dec","Z"}
            };

        public static string ConvertMonthFromDB2TT(string dbTickerMonth)
        {
            return TickerMonthFromTT2DB.FirstOrDefault(x => x.Value == dbTickerMonth).Key;
        }

        public static string ConvertFromTTAPIFields2DB(string productName,string instrumentName)
        {
            char[] delimiterChars = {' '};
            string[] words = instrumentName.Split();
            return TickerheadConverters.ConvertFromTT2DB(productName) + TickerMonthFromTT2DB[words[words.Count()-1].Substring(0,3)]+
                Convert.ToString(2000 + Int32.Parse(words[words.Count() - 1].Substring(3, 2)));
        }

        public static ttapiTicker ConvertFromDbTicker2ttapiTicker(string dbTicker)
        {
            ttapiTicker ttapiTickerOut = new ttapiTicker();
            ContractUtilities.ContractSpecs contractSpecsOut = ContractUtilities.ContractMetaInfo.GetContractSpecs(dbTicker);
            ttapiTickerOut.productName = TA.TickerheadConverters.ConvertFromDB2TT(contractSpecsOut.tickerHead);
            ttapiTickerOut.productType = "FUTURE";

            string exchangeName = ContractUtilities.ContractMetaInfo.GetExchange4Tickerhead(contractSpecsOut.tickerHead);
            ttapiTickerOut.marketKey = "";

            if (exchangeName == "ICE")
                ttapiTickerOut.marketKey = "ICE_IPE";
            else if (exchangeName == "CME")
                ttapiTickerOut.marketKey = "CME";

            ttapiTickerOut.SeriesKey = ConvertMonthFromDB2TT(contractSpecsOut.tickerMonthStr) + (contractSpecsOut.tickerYear % 100).ToString();

            ttapiTickerOut.instrumentName = ttapiTickerOut.marketKey + " " + 
                                    ttapiTickerOut.productName + " " +
                                    ttapiTickerOut.SeriesKey;

            return ttapiTickerOut;
        }


    }
}
