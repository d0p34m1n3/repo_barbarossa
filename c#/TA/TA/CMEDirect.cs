using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TA
{ 
    public static class CMEDirect
    {
        public static DataTable GetPriceFromCMEDirectFile()
        {
            string FilePath = DirectoryNames.GetDirectoryName(ext: "daily") + FileNames.CMEDirectIntradayPrice;
            string[] csvRows = System.IO.File.ReadAllLines(FilePath);
            string[] fields = null;

            DataTable PriceTable = new DataTable();
            PriceTable.Columns.Add("Ticker", typeof(string));
            PriceTable.Columns.Add("TickerHead", typeof(string));
            PriceTable.Columns.Add("BidPrice", typeof(double));
            PriceTable.Columns.Add("AskPrice", typeof(double));
            PriceTable.Columns.Add("MidPrice", typeof(double));

            double PriceMultiplier;

            for (int i = 1; i < csvRows.Length; i++)
            {
                fields = csvRows[i].Split(',');
                DataRow Row = PriceTable.NewRow();

                Row["Ticker"] = fields[0];
                Row["TickerHead"] = ContractUtilities.ContractMetaInfo.GetContractSpecs(fields[0]).tickerHead;

                if (Row.Field<string>("TickerHead") == "JY")
                {
                    PriceMultiplier = 1e7;
                }
                else
                {
                    PriceMultiplier = 1;
                }

                Row["BidPrice"] = Convert.ToDouble(fields[2]) * PriceMultiplier;
                Row["AskPrice"] = Convert.ToDouble(fields[3]) * PriceMultiplier;
                Row["MidPrice"] = (Row.Field<double>("BidPrice") + Row.Field<double>("AskPrice")) / 2;
                PriceTable.Rows.Add(Row);
            }

            return PriceTable;
        }
    }
}
