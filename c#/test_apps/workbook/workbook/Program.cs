using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using MySql.Data;
using MySql.Data.MySqlClient;
using System.Data;
using DatabaseConnection;
using TA;

namespace workbook
{
    class Program
    {
        static void Main(string[] args)
        {

            

            mysql connection = new mysql();
            MySqlConnection conn = connection.conn;

            DataTable myDT = TA.Strategy.getTrades(alias: "LNZ16V16VCS", conn: conn);

            myDT.Columns.Add("fullTicker", typeof(string));

            foreach (System.Data.DataRow row in myDT.Rows)
            {
                if (row["instrument"].ToString()=="F")
                {
                    row["fullTicker"] = row["ticker"];
                }
                else if (row["instrument"].ToString()=="O")
                {
                    row["fullTicker"] = row["ticker"].ToString() + "_" +
                        row["option_type"].ToString() + "_" + row["strike_price"].ToString();
                }
                

            }

            //DataTable wuhu = myDT.AsEnumerable().GroupBy(r => new { Col1 = r["fullTicker"] }).
             //   Select(g=>new {emre=g.}).CopyToDataTable();


            //wuhu = myDT.AsEnumerable().GroupBy(r=>r.Field<string>("fullTicker")).Select(g=> new {emre=g.Field<string>("fullTicker")}).CopyToDataTable();

           

            var result2 = myDT.AsEnumerable().GroupBy(r => r["fullTicker"]).Select(w => new
            {
                fullTicker = w.Key.ToString(),
                ticker = w.First()["ticker"].ToString(),
                optionType = w.First()["option_type"].ToString(),
                instrument = w.First()["instrument"].ToString(),
                strikePrice = w.First()["strike_price"].ToString(),
                qty = w.Sum(r => decimal.Parse(r["trade_quantity"].ToString()))
            }).ToList();

            
            //    


            DataTable netPosition = new DataTable();
            netPosition.Columns.Add("ticker", typeof(string));
            netPosition.Columns.Add("optionType", typeof(string));
            netPosition.Columns.Add("strikePrice", typeof(decimal));
            netPosition.Columns.Add("instrument", typeof(string));
            netPosition.Columns.Add("qty", typeof(decimal));

            for (int i = 0; i < result2.Count; i++)
            {
                netPosition.Rows.Add();
                netPosition.Rows[i]["ticker"] = result2[i].ticker;
                netPosition.Rows[i]["strikePrice"] = result2[i].strikePrice;
                netPosition.Rows[i]["optionType"] = result2[i].optionType;
                netPosition.Rows[i]["instrument"] = result2[i].instrument;
                netPosition.Rows[i]["qty"] = result2[i].qty;
            }






           
            Console.WriteLine("Emre");
        }


    }

    class Car
    {
        public string emre;
        public decimal qty;

    }



}
