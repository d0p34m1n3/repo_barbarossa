using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;

using MySql.Data;
using MySql.Data.MySqlClient;
using System.Data;
using DatabaseConnection;
using TA;
using IOUtilities;
using ContractUtilities;
using CalendarUtilities;
using GetPrice;
using DataAnalysis;
using Risk;
using Portfolio;
using InterestCurve;
using System.Globalization;
using OptionModels;
using QLNet;

namespace workbook
{
    public class Greeks
    {
        public double OptionPrice { get; set; }
        public double ImpliedVol { get; set; }
        public double Delta { get; set; }
        public double Vega { get; set; }
        public double Theta { get; set; }
        public int CalDte { get; set; }
        public double Gamma { get; set; }

        public Greeks()
        {
            OptionPrice = double.NaN;
            ImpliedVol = double.NaN;
            Delta = double.NaN;
            Vega = double.NaN;
            Theta = double.NaN;
            Gamma = double.NaN;

        }

    }



    class Program
    {

        static void Main(string[] args)
        {
           

            mysql connection = new mysql();
            MySqlConnection conn = connection.conn;

            //            DataTable PriceTable = GetPrice.GetFuturesPrice.getFuturesPrice4Ticker(tickerHead: "ED", conn: conn,
            //              dateTimeFrom: new DateTime(2016, 8, 2), dateTimeTo: new DateTime(2016, 8, 2));

            //        DateTime DateTimeFrom = new DateTime(2016,10,1);
            //  DateTime DateTimeTo = new DateTime(2017,2,1);

            // Worked perfectly on September 12th and 13th
            

            




           

            DataTable DeltaStrategyTable = (from x in StrategyTable.AsEnumerable()
                                             where x.Field<string>("StrategyClass").Contains("delta")
                                             select x).CopyToDataTable();
            




            string alias = "LCG17Z16VCS";

            List<string>HedgeNotes = TA.StrategyHedger.HedgeStrategyAgainstDelta(alias: alias, deltaAlias: "delta_jul16", conn: conn);

            



            


           



           
            

            
         

            


                string instrument = "options";

       

                Console.WriteLine("Emre");
            


        }
    }
}

   
