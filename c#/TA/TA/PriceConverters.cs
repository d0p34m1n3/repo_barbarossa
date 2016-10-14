using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TA
{
    public static class PriceConverters
    {
        static List<string> TickerHeadList1 = new List<string> {"CL", "BO", "ED", "ES", "NQ"};
        static List<string> TickerHeadList2 = new List<string> {"B", "KC", "SB", "CC", "CT", "OJ"};
        static List<string> TickerHeadList3 = new List<string> {"HO","RB", "AD", "CD", "EC", "BP" };
        static List<string> TickerHeadList4 = new List<string> {"LC", "LN", "FC", "NG", "SI"};
        static List<string> TickerHeadList5 = new List<string> {"C", "S", "KW", "W"};
        static List<string> TickerHeadList6 = new List<string> {"SM","GC"};
        static List<string> TickerHeadList7 = new List<string> { "TU", "FV","TY", "US" };

        static List<string> TickerHeadList8 = new List<string> { "HO_CL", "RB_CL","CL_B","B_CL"};
        static List<string> TickerHeadList9 = new List<string> {"C_W", "W_C","W_KW","KW_W"};
        static List<string> TickerHeadList10 = new List<string> {"S_BO_SM" };

        public static decimal? FromTT2DB(decimal ttPrice,string tickerHead)
        {
            decimal? DBPrice = null;

            if (TickerHeadList1.Contains(tickerHead))
            {
                DBPrice = ttPrice/100;
            }
            else if (TickerHeadList2.Contains(tickerHead))
            {
                DBPrice = ttPrice;
            }
            else if (TickerHeadList3.Contains(tickerHead))
            {
                DBPrice = ttPrice/10000;
            }
            else if (TickerHeadList4.Contains(tickerHead))
            {
                DBPrice = ttPrice/1000;
            }
            else if (TickerHeadList5.Contains(tickerHead))
            {
                int PriceSign = Math.Sign(ttPrice);
                decimal AbsPrice = Math.Abs(ttPrice);

                DBPrice = PriceSign * (Math.Floor(AbsPrice / 10) + (decimal)(0.125 * (((int)AbsPrice) % 10)));
            }
            else if (TickerHeadList6.Contains(tickerHead))
            {
                DBPrice = ttPrice/10;
            }
            else if (tickerHead=="JY")
            {
                DBPrice = ttPrice*10;
            }
            else if (TickerHeadList7.Contains(tickerHead))
            {
                int PriceSign = Math.Sign(ttPrice);
                decimal AbsPrice = Math.Abs(ttPrice);

                decimal aux1 = Math.Floor(AbsPrice / 1000);
                int aux2 = ((int)AbsPrice % 1000);
                decimal aux3 = Math.Floor((decimal)aux2 / 10);
                int aux4 = aux2 % 10;
                double aux5 = double.NaN;

                if (aux4 == 2)
                {
                    aux5 = 0.25;
                }
                else if (aux4 == 5)
                {
                    aux5 = 0.5;
                }
                else if (aux4 == 7)
                {
                    aux5 = 0.75;
                }
                else if (aux4 == 0)
                {
                    aux5 = 0;
                }
                DBPrice = PriceSign*(aux1 + (aux3 + (decimal)aux5) / 32);
            }

            return DBPrice;
        }

        public static decimal? FromTTAutoSpreader2DB(decimal ttPrice,string tickerHead)
        {
            decimal? DBPrice = null;

            if (TickerHeadList8.Contains(tickerHead))
            {
                DBPrice = ttPrice / 100;
            }
            else if (TickerHeadList9.Contains(tickerHead))
            {
                DBPrice = ttPrice * 100;
            }
            else if (TickerHeadList10.Contains(tickerHead))
            {
                DBPrice = ttPrice;
            }
            return DBPrice;
        }

    }
}
