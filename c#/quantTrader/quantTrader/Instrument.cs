using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace quantTrader
{
    struct InstumentSpecs
    {
        public string TickerHead { get; set; }
        public int TickerYear { get; set; }
        public string TickerMonthString { get; set; }
        public int TickerMonthNum { get; set; }
        public int ContINDX {get; set;}
    }


    class Instrument
    {
        public string Ticker { get; set; }
        public string InstrumentType {get; set;}
        public string OptionType { get; set; }
        public decimal? Strike { get; set; }

        static string FullLetterMonthList = "FGHJKMNQUVXZ";

        public Instrument(string ticker, string instrumentType, string optionType, decimal? strike)
        {
            Ticker = ticker;
            InstrumentType = instrumentType;
            OptionType = optionType;
            Strike = strike;
        }

        public static InstumentSpecs GetSpecs(string ticker)

        {
            InstumentSpecs SpecsOutput = new InstumentSpecs();

            SpecsOutput.TickerHead = ticker.Substring(0, ticker.Length-5);
            SpecsOutput.TickerYear = int.Parse(ticker.Substring(ticker.Length - 4, 4));
            SpecsOutput.TickerMonthString = ticker.Substring(ticker.Length - 5, 1);
            SpecsOutput.TickerMonthNum = ConvertMonthFromStringToNumber(SpecsOutput.TickerMonthString);
            SpecsOutput.ContINDX = 100 * SpecsOutput.TickerYear + SpecsOutput.TickerMonthNum;

            Console.WriteLine(SpecsOutput.TickerYear);
            Console.WriteLine(SpecsOutput.TickerHead);
            Console.WriteLine(SpecsOutput.TickerMonthString);
            Console.WriteLine(SpecsOutput.TickerMonthNum);
            Console.WriteLine(SpecsOutput.ContINDX);

            return SpecsOutput;


        }

        public static int ConvertMonthFromStringToNumber(string stringMonth)
        {
            return FullLetterMonthList.IndexOf(stringMonth) + 1;
        }
        
    }
}
