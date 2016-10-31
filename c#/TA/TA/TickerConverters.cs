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
            string[] words = instrumentName.Split();
            string TickerHead = TickerheadConverters.ConvertFromTT2DB(productName);
            string ExchangeName = ContractUtilities.ContractMetaInfo.GetExchange4Tickerhead(TickerHead);

            if (instrumentName.Contains("Calendar") && (ExchangeName=="CME"))
            {
                string MaturityString = words.Last();

                return TickerHead + TickerMonthFromTT2DB[MaturityString.Substring(0, 3)] +
                 Convert.ToString(2000 + Int32.Parse(MaturityString.Substring(3, 2))) + "-" +
                 TickerHead +
                 TickerMonthFromTT2DB[MaturityString.Substring(9, 3)] +
                 Convert.ToString(2000 + Int32.Parse(MaturityString.Substring(12, 2)));
            }
            else if (instrumentName.Contains("Spread") && (ExchangeName == "ICE"))
            {
                string MaturityString = words.Last();
                return TickerHead + TickerMonthFromTT2DB[MaturityString.Substring(0, 3)] +
                 Convert.ToString(2000 + Int32.Parse(MaturityString.Substring(3, 2))) + "-" +
                 TickerHead +
                 TickerMonthFromTT2DB[MaturityString.Substring(6, 3)] +
                 Convert.ToString(2000 + Int32.Parse(MaturityString.Substring(9, 2)));
            }
            else
            {
                return TickerHead + TickerMonthFromTT2DB[words[words.Count() - 1].Substring(0, 3)] +
                 Convert.ToString(2000 + Int32.Parse(words[words.Count() - 1].Substring(3, 2)));
            }
           
        }

        public static ttapiTicker ConvertFromDbTicker2ttapiTicker(string dbTicker)
        {
            string ProductType;

            if (dbTicker.Contains("-"))
            {
                ProductType = "SPREAD";
            }
            else
            {
                ProductType = "FUTURE";
            }

            return ConvertFromDbTicker2ttapiTicker(dbTicker, ProductType);

        }

        public static ttapiTicker ConvertFromDbTicker2ttapiTicker(string dbTicker, string productType)
        {
            ttapiTicker ttapiTickerOut = new ttapiTicker();
            string ProductType = productType.ToUpper();
            string exchangeName;

            if (ProductType=="FUTURE")
            {
                ContractUtilities.ContractSpecs contractSpecsOut = ContractUtilities.ContractMetaInfo.GetContractSpecs(dbTicker);
                ttapiTickerOut.productName = TA.TickerheadConverters.ConvertFromDB2TT(contractSpecsOut.tickerHead);
                ttapiTickerOut.productType = "FUTURE";

                exchangeName = ContractUtilities.ContractMetaInfo.GetExchange4Tickerhead(contractSpecsOut.tickerHead);
                
                ttapiTickerOut.SeriesKey = ConvertMonthFromDB2TT(contractSpecsOut.tickerMonthStr) + (contractSpecsOut.tickerYear % 100).ToString();

            }
            else if (ProductType=="SPREAD")
            {
                string[] TickerList = dbTicker.Split(new char[] {'-'});
                ContractUtilities.ContractSpecs contractSpecsOut = ContractUtilities.ContractMetaInfo.GetContractSpecs(TickerList[0]);
                ttapiTickerOut.productName = TA.TickerheadConverters.ConvertFromDB2TT(contractSpecsOut.tickerHead);
                ttapiTickerOut.productType = "SPREAD";
                string SeriesKey;

                exchangeName = ContractUtilities.ContractMetaInfo.GetExchange4Tickerhead(contractSpecsOut.tickerHead);

                if (exchangeName == "CME")
                {
                    SeriesKey = "Calendar: 1x" + ttapiTickerOut.productName + " " + ConvertMonthFromDB2TT(contractSpecsOut.tickerMonthStr) + (contractSpecsOut.tickerYear % 100).ToString();
                    for (int i = 1; i < TickerList.Length; i++)
                    {
                        contractSpecsOut = ContractUtilities.ContractMetaInfo.GetContractSpecs(TickerList[i]);
                        SeriesKey = SeriesKey + ":-1x" + ConvertMonthFromDB2TT(contractSpecsOut.tickerMonthStr) + (contractSpecsOut.tickerYear % 100).ToString();
                    }
                }
                else if (exchangeName == "ICE")
                {
                    SeriesKey = ttapiTickerOut.productName + " Spread " + ConvertMonthFromDB2TT(contractSpecsOut.tickerMonthStr) + (contractSpecsOut.tickerYear % 100).ToString();
                    for (int i = 1; i < TickerList.Length; i++)
                    {
                        contractSpecsOut = ContractUtilities.ContractMetaInfo.GetContractSpecs(TickerList[i]);
                        SeriesKey = SeriesKey + "/" + ConvertMonthFromDB2TT(contractSpecsOut.tickerMonthStr) + (contractSpecsOut.tickerYear % 100).ToString();
                    }
                }
                else
                {
                    SeriesKey = "";
                }
                
                ttapiTickerOut.SeriesKey = SeriesKey;
            }
            else
            {
                return ttapiTickerOut;
            }

            ttapiTickerOut.marketKey = "";
            if (exchangeName == "ICE")
                ttapiTickerOut.marketKey = "ICE_IPE";
            else if (exchangeName == "CME")
                ttapiTickerOut.marketKey = "CME";

            ttapiTickerOut.instrumentName = ttapiTickerOut.marketKey + " " +
                                        ttapiTickerOut.productName + " " +
                                        ttapiTickerOut.SeriesKey;
            return ttapiTickerOut;

        }


    }
}
