using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using IOUtilities;
using System.Data;
using System.Threading;

namespace quantTrader
{
    class Program
    {
        static void Main(string[] args)
        {


            //Strategy Strategy1 = new Strategy("LNV2016Z2016Q2016V2016", "FuturesButterfly");
            //Strategy Strategy2 = new Strategy("CU2017Z2017N2017U2017", "FuturesButterfly");

            

            //Console.WriteLine(table.Rows[0]["ticker1"]);
            //Console.WriteLine(table.Rows[0]["ticker2"]);
            //Console.WriteLine(table.Rows[0]["ticker3"]);

            //Instrument Instrument1 = new Instrument(table.Rows[0]["ticker1"].ToString(), "F", null, null);
            //Instrument Instrument2 = new Instrument(table.Rows[0]["ticker2"].ToString(), "F", null, null);
            //Instrument Instrument3 = new Instrument(table.Rows[0]["ticker3"].ToString(), "F", null, null);

            //InstumentSpecs InstrumentSpecs1 = Instrument.GetSpecs(Instrument1.Ticker);
            //InstumentSpecs InstrumentSpecs2 = Instrument.GetSpecs(Instrument2.Ticker);
            // InstrumentSpecs3 = Instrument.GetSpecs(Instrument3.Ticker);

            string ttUserId = "ekocatulum";
            string ttPassword = "pompei1789";

            TTAPIBasicFunctionality.TTAPIInitialize initialize = new TTAPIBasicFunctionality.TTAPIInitialize(ttUserId, ttPassword);





            FuturesButterflyPortfolio FutButtPort = new FuturesButterflyPortfolio();

            DataTable AllSheet = FutButtPort.SpreadSheet.Tables["All"];

            //List<string> s = AllSheet.AsEnumerable().Select(x => x["tickerHead"].ToString()).ToList();

            var s = AllSheet.AsEnumerable().Select(x => x["tickerHead"].ToString());

            var s2 = s.Distinct().ToList();

            Console.WriteLine(TA.TickerheadConverters.ConversionFromTT2DB["IPE e-Brent"].ToString());

            Console.WriteLine(TA.TickerheadConverters.ConversionFromTT2DB.FirstOrDefault(x => x.Value == "B").Key);

            

            // Check that the compiler settings are compatible with the version of TT API installed
            TTAPIArchitectureCheck archCheck = new TTAPIArchitectureCheck();
            if (archCheck.validate())
            {
                Console.WriteLine("Architecture check passed.");

                // Dictates whether TT API will be started on its own thread
                bool startOnSeparateThread = false;

                if (startOnSeparateThread)
                {
                    // Start TT API on a separate thread
                    TTAPIFunctions tf = new TTAPIFunctions(ttUserId, ttPassword);
                    Thread workerThread = new Thread(tf.Start);
                    workerThread.Name = "TT API Thread";
                    workerThread.Start();

                    // Insert other code here that will run on this thread
                }
                else
                {
                    // Start the TT API on the same thread
                    using (TTAPIFunctions tf = new TTAPIFunctions(ttUserId, ttPassword))
                    {
                        tf.Start();
                    }
                }
            }
            else
            {
                Console.WriteLine("Architecture check failed.  {0}", archCheck.ErrorString);
            }


   
            



  
            Console.ReadLine();

   
        }
    }


    class FuturesButterflyPortfolio
    {
        public DataSet SpreadSheet;
        public DataTable GoodSheet;

        public FuturesButterflyPortfolio(DateTime reportDate)
        {
            SpreadSheet = IOUtilities.ExcelDataReader.LoadFile("C:/Research/strategy_output/futures_butterfly/" + 
                CalendarUtilities.BusinessDays.GetDirectoryExtension(reportDate) + "/butterflies.xlsx");
            GoodSheet = SpreadSheet.Tables["good"];
        
        }

        public FuturesButterflyPortfolio()
            : this(CalendarUtilities.BusinessDays.GetBusinessDayShifted(-1))
        {   
        }
        

        public static string ConstructStrategyAlias(string ticker1, string ticker2, string ticker3, int direction)

        {
            InstumentSpecs InstrumentSpecs1 = Instrument.GetSpecs(ticker1);
            InstumentSpecs InstrumentSpecs2 = Instrument.GetSpecs(ticker2);
            InstumentSpecs InstrumentSpecs3 = Instrument.GetSpecs(ticker3);

            string StrategyAlias;

            if (direction > 0)

                StrategyAlias = InstrumentSpecs1.TickerHead +
                    InstrumentSpecs1.TickerMonthString + InstrumentSpecs1.TickerYear.ToString() +
                    InstrumentSpecs2.TickerMonthString + InstrumentSpecs2.TickerYear.ToString() +
                    InstrumentSpecs2.TickerMonthString + InstrumentSpecs2.TickerYear.ToString() +
                    InstrumentSpecs3.TickerMonthString + InstrumentSpecs3.TickerYear.ToString();
            else

                StrategyAlias = InstrumentSpecs1.TickerHead +
                    InstrumentSpecs2.TickerMonthString + InstrumentSpecs2.TickerYear.ToString() +
                    InstrumentSpecs3.TickerMonthString + InstrumentSpecs3.TickerYear.ToString() +
                    InstrumentSpecs1.TickerMonthString + InstrumentSpecs1.TickerYear.ToString() +
                    InstrumentSpecs2.TickerMonthString + InstrumentSpecs2.TickerYear.ToString(); 
                    
             return StrategyAlias;

        }
    }
}
