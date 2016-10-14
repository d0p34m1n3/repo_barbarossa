using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ContractUtilities
{
    public class ContractSpecs
    {
        public string tickerHead { get; set;}
        public int tickerYear { get; set; }
        public string tickerMonthStr { get; set; }
        public int tickerMonthNum { get; set; }
        public string tickerClass { get; set; }
        public int contINDX { get; set; }
    }

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

        public static List<string> AmericanExerciseTickerheadList =
            new List<string>(new string[] {"LN", "LC", "ES", "EC", "JY", "AD", "CD", "BP", "GC", "SI",
                       "TY", "US", "FV", "TU", "C", "S", "SM", "BO", "W", "CL", "NG",
                       "ED", "E0", "E2", "E3", "E4", "E5" });

        public static List<string> iceFuturesTickerheadList =
            new List<string>(new string[] { "SB", "KC", "CC", "CT", "OJ", "B" });

        public static Dictionary<string, string> tickerClassDict = new Dictionary<string, string>
            {
            {"LN","Livestock"},{"LC","Livestock"},{"FC","Livestock"},{"ES","Index"},{"NQ","Index"},
            {"EC","FX"},{"JY","FX"},{"AD","FX"},{"CD","FX"},{"BP","FX"},{"GC","Metal"},{"SI","Metal"},
            {"TY","Treasury"},{"US","Treasury"},{"FV","Treasury"},{"TU","Treasury"},
            {"C","Ag"},{"S","Ag"},{"SM","Ag"},{"BO","Ag"},{"W","Ag"},{"KW","Ag"},
            {"SB","Soft"},{"KC","Soft"},{"CC","Soft"},{"CT","Soft"},{"OJ","Soft"},
            {"CL","Energy"},{"B","Energy"},{"HO","Energy"},{"RB","Energy"},{"NG","Energy"},
            {"ED","STIR"},{"E0","STIR"},{"E2","STIR"},{"E3","STIR"},{"E4","STIR"},{"E5","STIR"}
            };

        public static List<string> fullLetterMonthList = new List<string> { "F", "G", "H", "J", "K", "M", "N", "Q", "U", "V", "X", "Z" };
        public static List<string> QuarterlyLetterMonthList = new List<string> { "H", "M", "U", "Z" };

        public static string GetExchange4Tickerhead(string tickerHead)
        {
            if (cmeFuturesTickerheadList.Contains(tickerHead))
                return "CME";
            else if (iceFuturesTickerheadList.Contains(tickerHead))
                return "ICE";
            else
                return null;
        }

        public static Dictionary<string, List<string>> FuturesContractMonths = new Dictionary<string, List<string>>
        {{"LN",new List<string>(new string[] {"G", "J", "K", "M", "N", "Q", "V", "Z"})},
         {"LC",new List<string>(new string[] {"G", "J", "M", "Q", "V", "Z"})},
         {"FC",new List<string>(new string[] {"F", "H", "J", "K", "Q", "U", "V", "X"})},
         {"C",new List<string>(new string[] {"H", "K", "N", "U", "Z"})},
         {"S",new List<string>(new string[] {"F", "H", "K", "N", "Q", "U", "X"})},
         {"SM",new List<string>(new string[] {"F", "H", "K", "N", "Q", "U", "V", "Z"})},
         {"BO",new List<string>(new string[] {"F", "H", "K", "N", "Q", "U", "V", "Z"})},
         {"W",new List<string>(new string[] {"H", "K", "N", "U", "Z"})},
         {"KW",new List<string>(new string[] {"H", "K", "N", "U", "Z"})},
         {"SB",new List<string>(new string[] {"H", "K", "N", "V"})},
         {"KC",new List<string>(new string[] {"H", "K", "N", "U", "Z"})},
         {"CC",new List<string>(new string[] {"H", "K", "N", "U", "Z"})},
         {"CT",new List<string>(new string[] {"H", "K", "N", "V", "Z"})},
         {"OJ",new List<string>(new string[] {"F", "H", "K", "N", "U", "X"})},
         {"ED",fullLetterMonthList},
         {"CL",fullLetterMonthList},
         {"B",fullLetterMonthList},
         {"HO",fullLetterMonthList},
         {"RB",fullLetterMonthList},
         {"NG",fullLetterMonthList},
         {"ES",QuarterlyLetterMonthList},
         {"NQ",QuarterlyLetterMonthList},
         {"EC",QuarterlyLetterMonthList},
         {"JY",QuarterlyLetterMonthList},
         {"AD",QuarterlyLetterMonthList},
         {"CD",QuarterlyLetterMonthList},
         {"BP",QuarterlyLetterMonthList},
         {"TY",QuarterlyLetterMonthList},
         {"US",QuarterlyLetterMonthList},
         {"FV",QuarterlyLetterMonthList},
         {"TU",QuarterlyLetterMonthList},
         {"GC",fullLetterMonthList},
         {"SI",fullLetterMonthList}};

        public static Dictionary<string, double> ContractMultiplier = new Dictionary<string, double>
        {{"LN", 400},
         {"LC", 400},
         {"FC", 500},
         {"C", 50},
         {"S", 50},
         {"SM", 100},
         {"BO", 600},
         {"W", 50},
         {"KW", 50},
         {"SB", 1120},
         {"KC", 375},
         {"CC", 10},
         {"CT", 500},
         {"OJ", 150},
         {"CL", 1000},
         {"B", 1000},
         {"HO", 42000},
         {"RB", 42000},
         {"NG", 10000},
         {"ED", 2500},
         {"E0", 2500},
         {"E2", 2500},
         {"E3", 2500},
         {"E4", 2500},
         {"E5", 2500},
         {"ES", 50},
         {"NQ", 20},
         {"AD", 100000},
         {"CD", 100000},
         {"EC", 125000},
         {"JY", 1.25},
         {"BP", 62500},
         {"FV", 1000},
         {"TU", 2000},
         {"TY", 1000},
         {"US", 1000},
         {"GC", 100},
         {"SI", 5000}};

        public static string GetOptionExerciseType(string tickerHead)
        {
            if (AmericanExerciseTickerheadList.Contains(tickerHead))
            {
                return "A";
            }
            else
            {
                return "";
            }
        }


        public static ContractSpecs GetContractSpecs(string ticker)
        {
            ContractSpecs specsOutput = new ContractSpecs();
            specsOutput.tickerHead = ticker.Substring(0, ticker.Count() - 5);
            specsOutput.tickerYear = Convert.ToInt16(ticker.Substring(ticker.Count() - 4, 4));
            specsOutput.tickerMonthStr = ticker.Substring(ticker.Count() - 5, 1);
            specsOutput.tickerMonthNum = fullLetterMonthList.FindIndex(x => x == specsOutput.tickerMonthStr) + 1;
            specsOutput.tickerClass = tickerClassDict[specsOutput.tickerHead];
            specsOutput.contINDX = 100*specsOutput.tickerYear + specsOutput.tickerMonthNum;
            return specsOutput;
        }      
    }
}
