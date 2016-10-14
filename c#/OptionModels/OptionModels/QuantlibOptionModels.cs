using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QLNet;

namespace OptionModels
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

    public class AugmentedGreeks:Greeks
    {
        public double DollarVega {get; set;}
        public double DollarTheta {get; set;}
        public double DollarGamma {get; set;}
        public double InterestRate {get; set;}
    }

    public static class QuantlibOptionModels
    {
        public static Greeks GetOptionOnFutureGreeks(double underlyingPrice,double strike,double riskFreeRate, 
            DateTime expirationDate, DateTime calculationDate, string optionType, string exerciseType,
            double optionPrice=double.NaN,double impliedVol=0.15,string engineName="baw")
        {
            QLNet.Date ExpirationDateObj = new QLNet.Date(expirationDate.Day, expirationDate.Month, expirationDate.Year);
            QLNet.Date CalculationDateObj = new QLNet.Date(calculationDate.Day, calculationDate.Month, calculationDate.Year);

            QLNet.DayCounter DayCountObj = new QLNet.Actual365Fixed();
            QLNet.Calendar CalendarObj = new QLNet.UnitedStates();

            Greeks GreeksOutput = new Greeks();
            QLNet.Option.Type OptionTypeObj;
            QLNet.Exercise ExerciseObj;
            double ImpliedVol;
            double OptionPrice;

            int CalDte = DayCountObj.dayCount(CalculationDateObj, ExpirationDateObj);
            GreeksOutput.CalDte = CalDte;

            if (!double.IsNaN(optionPrice))
            {
                if (optionType.ToUpper() == "C")
                {
                    if (optionPrice + strike - underlyingPrice <= 1.0e-12)
                    {
                        GreeksOutput.Delta = 1;
                        return GreeksOutput;
                    }
                }
                else if (optionType.ToUpper() == "P")
                {
                    if (optionPrice - strike + underlyingPrice <= 1.0e-12)
                    {
                        GreeksOutput.Delta = -1;
                        return GreeksOutput;
                    }
                }
            }

            if (CalDte == 0)
            {
                if (optionType.ToUpper() == "C")
                {
                    if (strike <= underlyingPrice)
                    {
                        GreeksOutput.Delta = 1;
                    }
                    else
                    {
                        GreeksOutput.Delta = 0;
                    }
                }
                else if (optionType.ToUpper() == "P")
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
                return GreeksOutput;
            }

            if (optionType.ToUpper() == "C")
            {
                OptionTypeObj = QLNet.Option.Type.Call;
            }
            else if (optionType.ToUpper() == "P")
            {
                OptionTypeObj = QLNet.Option.Type.Put;
            }
            else
            {
                return GreeksOutput;
            }

            if (exerciseType.ToUpper() == "E")
            {
                ExerciseObj = new QLNet.EuropeanExercise(ExpirationDateObj);
            }
            else if (exerciseType.ToUpper() == "A")
            {
                ExerciseObj = new QLNet.AmericanExercise(CalculationDateObj, ExpirationDateObj);
            }
            else
            {
                return GreeksOutput;
            }

            QLNet.Settings.setEvaluationDate(CalculationDateObj);

            QLNet.Handle<Quote> UnderlyingObj = new QLNet.Handle<Quote>(new QLNet.SimpleQuote(underlyingPrice));
            QLNet.Handle<YieldTermStructure> FlatRateObj = new QLNet.Handle<YieldTermStructure>(new QLNet.FlatForward(CalculationDateObj, riskFreeRate, DayCountObj));
            QLNet.Handle<BlackVolTermStructure> FlatVolTsObj = new QLNet.Handle<BlackVolTermStructure>(new QLNet.BlackConstantVol(CalculationDateObj, CalendarObj, impliedVol, DayCountObj));

            QLNet.BlackProcess BlackProc = new QLNet.BlackProcess(UnderlyingObj, FlatRateObj, FlatVolTsObj);
            QLNet.PlainVanillaPayoff PayoffObj = new QLNet.PlainVanillaPayoff(OptionTypeObj, strike);

            QLNet.VanillaOption OptionObj = new QLNet.VanillaOption(PayoffObj, ExerciseObj);

            if (engineName == "baw")
            {
                OptionObj.setPricingEngine(new QLNet.BaroneAdesiWhaleyApproximationEngine(BlackProc));
            }
            else if (engineName == "fda")
            {
                OptionObj.setPricingEngine(new QLNet.FDAmericanEngine(BlackProc, 100, 100));
            }
            else
            {
                return GreeksOutput;
            }


            if (!double.IsNaN(optionPrice))
            {
                try
                {

                    ImpliedVol = OptionObj.impliedVolatility(targetValue:optionPrice, process:BlackProc,accuracy:1e-5);
                }
                catch
                {
                    return GreeksOutput;
                }
                
                FlatVolTsObj = new QLNet.Handle<BlackVolTermStructure>(new QLNet.BlackConstantVol(CalculationDateObj, CalendarObj, ImpliedVol, DayCountObj));
                BlackProc = new QLNet.BlackProcess(UnderlyingObj, FlatRateObj, FlatVolTsObj);

                if (engineName == "baw")
                {
                    OptionObj.setPricingEngine(new QLNet.BaroneAdesiWhaleyApproximationEngine(BlackProc));
                }
                else if (engineName == "fda")
                {
                    OptionObj.setPricingEngine(new QLNet.FDAmericanEngine(BlackProc, 100, 100));
                }
                OptionPrice = optionPrice;
            }
            else
            {
                OptionPrice = OptionObj.NPV();
                ImpliedVol = impliedVol;
            }

            OptionObj = new QLNet.VanillaOption(PayoffObj, new QLNet.EuropeanExercise(ExpirationDateObj));
            OptionObj.setPricingEngine(new QLNet.AnalyticEuropeanEngine(BlackProc));

            GreeksOutput.Delta = OptionObj.delta();
            GreeksOutput.Vega = OptionObj.vega();
            GreeksOutput.Theta = OptionObj.thetaPerDay();
            GreeksOutput.Gamma = OptionObj.gamma();
            GreeksOutput.OptionPrice = OptionPrice;
            GreeksOutput.ImpliedVol = ImpliedVol;

            return GreeksOutput;

        }
    }
}
