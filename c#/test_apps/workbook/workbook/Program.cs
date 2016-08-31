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
        public double OptionPrice {get; set;}
        public double ImpliedVol {get; set;}
        public double Delta { get; set;}
        public double Vega { get; set;}
        public double Theta { get; set;}
        public int CalDte {get; set;}
        public double Gamma {get; set;}

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

            string Ticker = "SIZ2016";
            DateTime InterestRateDate = new DateTime(2016, 8, 11);
            DateTime CalculationDate = new DateTime(2016, 8, 11);
            double optionPrice = 1.20300000;
            double Option_price;
            string optionType = "C";
            double strike = 20;
            double impliedVol = 0.15;
            string engineName = "baw";
            double DividendRate = 0;

            
            QLNet.Option.Type OptionTypeObj;
            QLNet.Exercise ExerciseObj;
//double underlying = 

            Greeks GreeksOutput = new Greeks();

            DateTime ExpitationDate = (DateTime)Expiration.getExpirationFromDB(ticker: Ticker, instrument: "options", conn: conn);

            double InterestRate = InterestCurve.RateFromStir.GetSimpleRate(asOfDate: InterestRateDate, dateFrom: InterestRateDate,
                dateTo: ExpitationDate, tickerHead: "ED", conn: conn);


            QLNet.Date ExpitationDateQL = new QLNet.Date(ExpitationDate.Day, ExpitationDate.Month, ExpitationDate.Year);
            QLNet.Date CalculationDateQL = new QLNet.Date(CalculationDate.Day, CalculationDate.Month, CalculationDate.Year);

            QLNet.DayCounter DayCountObj = new QLNet.Actual365Fixed();
            QLNet.Calendar CalendarObj = new QLNet.UnitedStates();

            int CalDte = DayCountObj.dayCount(CalculationDateQL, ExpitationDateQL);
            GreeksOutput.CalDte = CalDte;

            //if Double.IsNaN(optionPrice)

            List<string> MyList = new List<string>(new string[] { "G", "J", "K", "M", "N", "Q", "V", "Z" });
            List<int> ContractMonthNumbers = new List<int>();
            
            foreach (string item in MyList)
	{
		 ContractMonthNumbers.Add(ContractUtilities.ContractMetaInfo.fullLetterMonthList.FindIndex(x => x == item) + 1);
	}

            string UnderlyingTicker = OptionModels.Utils.GetOptionUnderlying(Ticker);

            double underlyingPrice = (double)(GetPrice.GetFuturesPrice.getFuturesPrice4Ticker(ticker: UnderlyingTicker,
                dateTimeFrom: CalculationDate, dateTimeTo: CalculationDate, conn: conn).Rows[0].Field<decimal>("close_price"));

            ContractUtilities.ContractSpecs ContractSpecsOut = ContractUtilities.ContractMetaInfo.GetContractSpecs(ticker: UnderlyingTicker);


      if (!double.IsNaN(optionPrice))
      {
          if (optionType=="C")
          {
              if (optionPrice+strike-underlyingPrice <= Math.Pow(10,-12))
              {
                  GreeksOutput.Delta = 1;
              }
          }
          else if (optionType=="P")
          {
              if (optionPrice - strike + underlyingPrice <= Math.Pow(10, -12))
              {
                  GreeksOutput.Delta = -1;
              }
          }
      }

            if (CalDte==0)
            {
                if (optionType=="C")
                {
                    if (strike<=underlyingPrice)
                    {
                        GreeksOutput.Delta = 1;
                    }
                    else
                    {
                        GreeksOutput.Delta = 0;
                    }
                }
                else if (optionType=="P")
                {
                    if (strike >= underlyingPrice)
                    {
                        GreeksOutput.Delta = -1;
                    }
                    else
                    {
                        GreeksOutput.Delta = 0;
                    }
                }
            }

            if (optionType.ToUpper()=="C")
            {
                OptionTypeObj = QLNet.Option.Type.Call;
            }
            else if (optionType.ToUpper()=="P")
            {
                OptionTypeObj = QLNet.Option.Type.Put;
            }
            else
            {
                return;
            }
          
            string ExerciseType = ContractUtilities.ContractMetaInfo.GetOptionExerciseType(ContractSpecsOut.tickerHead);
            
            if (ExerciseType.ToUpper()=="E")
            {
               ExerciseObj = new QLNet.EuropeanExercise(ExpitationDateQL);
            }
            else if  (ExerciseType.ToUpper()=="A")
            {
                ExerciseObj = new QLNet.AmericanExercise(CalculationDateQL,ExpitationDateQL);
            }
            else
            {
                return;
            }

            QLNet.Settings.setEvaluationDate(CalculationDateQL);

            QLNet.Handle<Quote> UnderlyingObj = new QLNet.Handle<Quote>(new QLNet.SimpleQuote(underlyingPrice));



            QLNet.Handle<YieldTermStructure> FlatRateObj = new QLNet.Handle<YieldTermStructure>(new QLNet.FlatForward(CalculationDateQL, InterestRate, DayCountObj));
            QLNet.Handle<YieldTermStructure> DividendYieldObj = new QLNet.Handle<YieldTermStructure>(new QLNet.FlatForward(CalculationDateQL, DividendRate, DayCountObj));

            QLNet.Handle<BlackVolTermStructure> FlatVolTsObj = new QLNet.Handle<BlackVolTermStructure>(new QLNet.BlackConstantVol(CalculationDateQL, CalendarObj, impliedVol, DayCountObj));

            QLNet.BlackProcess BlackProc = new QLNet.BlackProcess(UnderlyingObj, FlatRateObj, FlatVolTsObj);

            QLNet.PlainVanillaPayoff PayoffObj = new QLNet.PlainVanillaPayoff(OptionTypeObj, strike);

            QLNet.VanillaOption OptionObj = new QLNet.VanillaOption(PayoffObj, ExerciseObj);

            if (engineName=="baw")
            {
                OptionObj.setPricingEngine(new QLNet.BaroneAdesiWhaleyApproximationEngine(BlackProc));
            }
            else if (engineName == "fda")
            {
                OptionObj.setPricingEngine(new QLNet.FDAmericanEngine(BlackProc, 100, 100));
            }
            else
            {
                return;
            }

            double OptionPrice = OptionObj.NPV();
            double ImpliedVol;

            if (!double.IsNaN(optionPrice))
            {
                ImpliedVol = OptionObj.impliedVolatility(optionPrice, BlackProc,Math.Pow(10,-5),100,0.01,4);
                FlatVolTsObj = new QLNet.Handle<BlackVolTermStructure>(new QLNet.BlackConstantVol(CalculationDateQL, CalendarObj, ImpliedVol, DayCountObj));
                BlackProc = new QLNet.BlackProcess(UnderlyingObj, FlatRateObj, FlatVolTsObj);

                if (engineName=="baw")
            {
                OptionObj.setPricingEngine(new QLNet.BaroneAdesiWhaleyApproximationEngine(BlackProc));
            }
            else if (engineName == "fda")
            {
                OptionObj.setPricingEngine(new QLNet.FDAmericanEngine(BlackProc, 100, 100));
            }
            else
            {
                return;
            }

                OptionPrice = OptionObj.NPV();

            }


            GreeksOutput.Delta = OptionObj.delta();
            GreeksOutput.Vega = OptionObj.vega();
            GreeksOutput.Theta = OptionObj.thetaPerDay();
            GreeksOutput.Gamma = OptionObj.gamma();


            

          //  double RateOutput;
                
            //DataTable wuhu = myDT.AsEnumerable().GroupBy(r => new { Col1 = r["fullTicker"] }).
             //   Select(g=>new {emre=g.}).CopyToDataTable();


            //wuhu = myDT.AsEnumerable().GroupBy(r=>r.Field<string>("fullTicker")).Select(g=> new {emre=g.Field<string>("fullTicker")}).CopyToDataTable();

            //QLNet.Option.Type type = QLNet.Option.Type.Call;



            string instrument = "options";
            

            //Nullable<DateTime>myExp =  ContractUtilities.Expiration.getExpirationFromDB(ticker, instrument, conn);



            

               
            

            

            

            
            //    


            






           
            Console.WriteLine("Emre");
        }


    }

   


}
