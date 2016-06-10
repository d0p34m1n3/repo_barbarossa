using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TA
{
    public static class TickerheadConverters
    {
        public static Dictionary<string, string> ConversionFromTT2DB =
            new Dictionary<string, string>()
            {
            {"CL","CL"},{"HO","HO"},{"RB","RB"},{"NG","NG"},{"GE","ED"},
            {"ZC","C"},{"ZW","W"},{"ZS","S"},{"ZM","SM"},{"ZL","BO"},{"KE","KW"},
            {"LE","LC"},{"HE","LN"},{"GF","FC"},
            {"IPE e-Brent","B"},{"Coffee C","KC"},{"Cocoa", "CC"},{"Sugar No 11","SB"},{"Cotton No 2","CT"}
            };

        public static string ConvertFromTT2DB(string ttTickerHead)
        {
            return ConversionFromTT2DB[ttTickerHead].ToString();
        }

        public static string ConvertFromDB2TT(string dbTickerHead)
        {
            return ConversionFromTT2DB.FirstOrDefault(x => x.Value == dbTickerHead).Key;
        }
    }
}
