using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OptionModels
{
    public static class Tests
    {
        public static bool TestAmericanBlack(MySqlConnection conn)
        {
            DataSet TestDataSet = IOUtilities.ExcelDataReader.LoadFile("C:/Research/data/test_data/option_model_test.xlsx");

            DataTable TestDataTable = TestDataSet.Tables["All"];
            TestDataTable.Columns.Add("ImpVolDiff", typeof(Double));
            TestDataTable.Columns.Add("DeltaDiff", typeof(Double));
            TestDataTable.Columns.Add("DollarVegaDiff", typeof(Double));
            TestDataTable.Columns.Add("DollarThetaDiff", typeof(Double));
            TestDataTable.Columns.Add("DollarGammaDiff", typeof(Double));
            TestDataTable.Columns.Add("RateDiff", typeof(Double));
            AugmentedGreeks GreeksOut;

            foreach (DataRow Row in TestDataTable.Rows)
            {
                GreeksOut = OptionModels.Utils.OptionModelWrapper(modelName: "Black", ticker: Row.Field<string>("ticker"),
                optionType: Row.Field<string>("option_type"), strike: Row.Field<double>("strike"), conn: conn,
                calculationDate: DateTime.ParseExact(Row.Field<double>("settleDates").ToString(), "yyyyMMdd", CultureInfo.InvariantCulture),
                optionPrice: Row.Field<double>("theoValue"), underlyingPrice: Row.Field<double>("underlying"));

                Row["ImpVolDiff"] = 100 * Math.Abs(GreeksOut.ImpliedVol - Row.Field<double>("impVol")) / Row.Field<double>("impVol");

                Row["DeltaDiff"] = 100 * Math.Abs(GreeksOut.Delta - Row.Field<double>("delta")) / Row.Field<double>("delta");
                Row["DollarVegaDiff"] = 100 * Math.Abs(GreeksOut.DollarVega - Row.Field<double>("dollarVega")) / Row.Field<double>("dollarVega");
                Row["DollarThetaDiff"] = 100 * Math.Abs(GreeksOut.DollarTheta - Row.Field<double>("dollarTheta")) / Row.Field<double>("dollarTheta");
                Row["DollarGammaDiff"] = 100 * Math.Abs(GreeksOut.DollarGamma - Row.Field<double>("dollarGamma")) / Row.Field<double>("dollarGamma");
                Row["RateDiff"] = 100 * Math.Abs(GreeksOut.InterestRate - Row.Field<double>("rate2OptExp")) / Row.Field<double>("rate2OptExp");
            }

            int NumLargeDiff = TestDataTable.Select("ImpVolDiff>0.5 OR DeltaDiff>0.5 OR DollarVegaDiff>0.5 OR DollarThetaDiff>0.5 OR DollarGammaDiff>0.5 OR RateDiff>0.5").Length;

            if (NumLargeDiff == 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }
        }   
}
