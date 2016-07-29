using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ContractUtilities
{
    public static class ContractMetaInfo
    {
        public static List<string> FuturesButterflyTickerheadList = 
            new List<string> (new string[] {"LN", "LC", "FC",
                                              "C", "S", "SM", "BO", "W", "KW",
                                              "SB", "KC", "CC", "CT", "OJ",
                                              "CL", "B", "HO", "RB", "NG", "ED"});

        public static List<string> cmeFuturesTickerheadList = 
            new List<string> (new string[] {"LN", "LC", "FC",
                               "C", "S", "SM", "BO", "W", "KW",
                               "CL", "HO", "RB", "NG", "ED",
                               "ES", "NQ", "EC", "JY", "AD", "CD", "BP",
                               "TY", "US", "FV", "TU", "GC", "SI"});

        public static List<string> iceFuturesTickerheadList =
            new List<string>(new string[] { "SB", "KC", "CC", "CT", "OJ", "B" });


        public static string GetExchange4Tickerhead(string tickerHead)
        {
            if (cmeFuturesTickerheadList.Contains(tickerHead))
                return "CME";
            else if (iceFuturesTickerheadList.Contains(tickerHead))
                return "ICE";
            else
                return null;
        }
            
    }
}
