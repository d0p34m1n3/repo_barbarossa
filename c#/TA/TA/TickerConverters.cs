using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TA
{
    public static class TickerConverters
    {
        public static Dictionary<string, string> TickerMonthFromTT2DB =
            new Dictionary<string, string>()
            {
            {"Jan","F"},{"Feb","G"},{"Mar","H"},{"Apr","J"},
            {"May","K"},{"Jun","M"},{"Jul","N"},{"Aug","Q"},
            {"Sep","U"},{"Oct","V"},{"Nov","X"},{"Dec","Z"}
            };

        public static string ConvertFromTTAPIFields2DB(string productName,string instrumentName)
        {
            char[] delimiterChars = {' '};
            string[] words = instrumentName.Split();
            return TickerheadConverters.ConvertFromTT2DB(productName) + TickerMonthFromTT2DB[words[words.Count()-1].Substring(0,3)]+
                Convert.ToString(2000 + Int32.Parse(words[words.Count() - 1].Substring(3, 2)));
        }
    }
}
