using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TA
{
    public static class FileNames
    {
        public static string ttapi_credential_file
        {
            get
            {
                return "TTAPI_Credentials.txt";
            }
        }

        public static string candlestick_signal_file
        {
            get
            {
                return "candlestick.xml";
            }
        }
        public static string CMEDirectIntradayPrice
        {
            get
            {
                return "cme_direct_prices.csv";
            }
        }
    }
}
