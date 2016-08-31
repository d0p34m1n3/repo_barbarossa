using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data;
using MySql.Data.MySqlClient;
using GetPrice;
using System.Data;
using DataAnalysis;
using ContractUtilities;
using TA;
using IOUtilities;
using System.Globalization;

namespace InterestCurve
{
    public static class RateFromStir
    {
        public static double GetSimpleRate(DateTime asOfDate, DateTime dateFrom, DateTime dateTo, string tickerHead,MySqlConnection conn)
        {

            string TAOutputDir = TA.DirectoryNames.GetDirectoryName(ext: "ta") + TA.DirectoryNames.GetDirectoryExtension(directoryDate: asOfDate);
            string FileName = TAOutputDir + "/" + tickerHead + "_InterestCurve.xml";
            DataTable PriceTable;
            

            if (System.IO.File.Exists(FileName))
            {
                PriceTable = new DataTable();
                PriceTable.ReadXml(FileName);
            }

            else
            {
                PriceTable = GetPrice.GetFuturesPrice.getFuturesPrice4Ticker(tickerHead: tickerHead, conn: conn,
               dateTimeFrom: asOfDate, dateTimeTo: asOfDate);

                DataAnalysis.DataTableFunctions.CleanNullRows(dataTableInput: PriceTable, columnNames: new string[] { "close_price" });

                PriceTable.Columns.Add("exp_date", typeof(DateTime));
                PriceTable.Columns.Add("implied_rate", typeof(Double));

                foreach (DataRow row in PriceTable.Rows)
                {
                    row["exp_date"] = ContractUtilities.Expiration.getExpirationFromDB(ticker: row.Field<string>("ticker"), instrument: "futures", conn: conn);
                    row["implied_rate"] = 100 - row.Field<decimal>("close_price");
                }

                PriceTable.TableName = "PriceTable";
                PriceTable.WriteXml(FileName, XmlWriteMode.WriteSchema);
            }

           

            if (PriceTable.Rows.Count==0)
            {
                return double.NaN;
            }

            DataRow[] PriceTableFirstRows = PriceTable.Select("exp_date<= #" + dateFrom.ToString("yyyy-MM-dd") + "#");
            DataRow[] PriceTableMiddleRows = PriceTable.Select("exp_date> #" + dateFrom.ToString("yyyy-MM-dd") +
                "# AND exp_date< #" + dateTo.ToString("yyyy-MM-dd") + "#");

            double RateOutput;

            if (PriceTableMiddleRows.Count() == 0)
            {
                if (PriceTableFirstRows.Count() > 0)
                {
                    RateOutput = PriceTableFirstRows[PriceTableFirstRows.Count() - 1].Field<double>("implied_rate") / 100;
                }
                else
                {
                    RateOutput = PriceTable.Rows[0].Field<double>("implied_rate") / 100;
                }
                return RateOutput;
            }

            double FirstRate;
            int FirstPeriod;
            double MiddleDiscount = 1;

            if (PriceTableFirstRows.Count() == 0)
            {
                FirstRate = PriceTableMiddleRows[0].Field<double>("implied_rate");
                FirstPeriod = (PriceTableMiddleRows[0].Field<DateTime>("exp_date") - dateFrom).Days;
            }
            else
            {
                FirstRate = PriceTableFirstRows[PriceTableFirstRows.Count() - 1].Field<double>("implied_rate");
                FirstPeriod = (PriceTableMiddleRows[0].Field<DateTime>("exp_date") - dateFrom).Days;
            }

            double LastRate = PriceTableMiddleRows[PriceTableMiddleRows.Count() - 1].Field<double>("implied_rate");
            int LastPeriod = (dateTo - PriceTableMiddleRows[PriceTableMiddleRows.Count() - 1].Field<DateTime>("exp_date")).Days;

            if (PriceTableMiddleRows.Count() > 1)
            {
                for (int i = 0; i < PriceTableMiddleRows.Count() - 1; i++)
                {
                    MiddleDiscount *= 1 + (PriceTableMiddleRows[i].Field<double>("implied_rate") *
                        (PriceTableMiddleRows[i + 1].Field<int>("cal_dte") - PriceTableMiddleRows[i].Field<int>("cal_dte")) / 36500);
                }
            }

            double TotalDiscount = (1 + (FirstRate * FirstPeriod / 36500)) * MiddleDiscount * (1 + (LastRate * LastPeriod / 36500));

            int TotalPeriod = PriceTableMiddleRows[PriceTableMiddleRows.Count() - 1].Field<int>("cal_dte") -
                PriceTableMiddleRows[0].Field<int>("cal_dte") + FirstPeriod + LastPeriod;

            RateOutput = (TotalDiscount - 1) * 365 / TotalPeriod;

            return RateOutput;
        }

        public static bool TestGetSimpleRate(MySqlConnection conn)
        {
            DataSet MyDataSet = ExcelDataReader.LoadFile("C:/Research/data/test_data/stir_option_rate_test.xlsx");
            DataTable MyDataTable = MyDataSet.Tables["All"];

            MyDataTable.Columns.Add("int_rate2", typeof(Double));
            MyDataTable.Columns.Add("rate_diff", typeof(Double));

            foreach (DataRow row in MyDataTable.Rows)
            {
                string ExpDate = row.Field<double>("exp_date").ToString();
                string Settledate = row.Field<double>("settle_date").ToString();
                DateTime ExpDatetime = DateTime.ParseExact(ExpDate, "yyyyMMdd", CultureInfo.InvariantCulture);
                DateTime SettleDatetime = DateTime.ParseExact(Settledate, "yyyyMMdd", CultureInfo.InvariantCulture);
                row["int_rate2"] = RateFromStir.GetSimpleRate(SettleDatetime, SettleDatetime, ExpDatetime, "ED", conn);
                row["rate_diff"] = row.Field<double>("int_rate") - row.Field<double>("int_rate2");
            }

            int NumLargeDiff = MyDataTable.Select("rate_diff>0.0000000001 AND rate_diff<-0.0000000001").Length;

            if (NumLargeDiff==0)
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
