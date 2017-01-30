using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TA
{
    public static class DirectoryNames
    {
        public static string rootWork = "C:/Research";

        public static Dictionary<string,string> extensionDict = new Dictionary<string,string>
        {
            {"ttapiContractVolume" , "/data/intraday_data/tt_api/"},
            {"ttapiBidAsk", "/data/intraday_data/tt_api/"},
            {"ttapiConfig", "/c#/config/"},
            {"overnightCandlestick", "/strategy_output/ibo/"},
            {"presavedFuturesData", "/data/FuturesData"},
            {"ta", "/ta/"},
            {"daily","/daily/"},
            {"ifsOutput","/strategy_output/ifs"},
            {"futuresButterflyOutput","/strategy_output/futures_butterfly"}
        };

        public static string GetDirectoryName(string ext)
        {
            return rootWork + extensionDict[ext];
        }

        public static string GetDirectoryExtension(DateTime directoryDate)
        {
            return directoryDate.Year.ToString() + "/" +
                (100 * directoryDate.Year + directoryDate.Month).ToString() + "/" +
                directoryDate.Date.ToString("yyyyMMdd");
        }
    }
}
