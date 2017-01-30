using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TechnicalSignals
{
    public class StochasticOscillator
    {
        List<DateTime> EndTimeList;
        DateTime currentTime;
        int LookBack1;
        int LookBack2;
        int LookBack3;

        public DataTable Data
        {
            get;
            set;
        }

        public StochasticOscillator(DateTime startDate, DateTime endDate, string[] instrumentList, int minInterval,int lookBack1,
            int lookBack2, int lookBack3)
        {
            LookBack1 = lookBack1;
            LookBack2 = lookBack2;
            LookBack3 = lookBack3;

            currentTime = startDate;
            EndTimeList = new List<DateTime>();
            Data = new DataTable();
            Data.Columns.Add("TimeStamp", typeof(DateTime));

            while (currentTime <= endDate)
            {
                EndTimeList.Add(currentTime);
                currentTime = currentTime.AddMinutes(minInterval);
            }

            for (int i = 0; i < instrumentList.Length; i++)
            {
                Data.Columns.Add(instrumentList[i] + "_hh", typeof(double));
                Data.Columns.Add(instrumentList[i] + "_ll", typeof(double));
                Data.Columns.Add(instrumentList[i] + "_K", typeof(double));
                Data.Columns.Add(instrumentList[i] + "_D1", typeof(double));
                Data.Columns.Add(instrumentList[i] + "_D2", typeof(double));
            }

            for (int i = 0; i < EndTimeList.Count; i++)
            {
                Data.Rows.Add();
                Data.Rows[i][0] = EndTimeList[i];

                for (int j = 0; j < instrumentList.Length; j++)
                {
                    Data.Rows[i][5 * j + 1] = Double.NaN;
                    Data.Rows[i][5 * j + 2] = Double.NaN;
                    Data.Rows[i][5 * j + 3] = Double.NaN;
                    Data.Rows[i][5 * j + 4] = Double.NaN;
                    Data.Rows[i][5 * j + 5] = Double.NaN;
                }
            }
        }

        public void UpdateValues(DateTime newDateTime,DataTable candleStickTable, string instrument)
        {
            List<DateTime> TimeStampList = DataAnalysis.DataTableFunctions.GetColumnAsList<DateTime>(dataTableInput: candleStickTable, 
                columnName: "end");
            int CandleIndex = TimeStampList.IndexOf(newDateTime);
            int StoIndex = EndTimeList.IndexOf(newDateTime);

            if (CandleIndex < LookBack1 - 1)
            {
                return;
            }

            DataTable CandleStickTableSelected = candleStickTable.AsEnumerable().Skip(CandleIndex - LookBack1 + 1).Take(LookBack1).CopyToDataTable();

            var QueryResult = (from CandleRow in CandleStickTableSelected.AsEnumerable()
                               where ((!Double.IsNaN(CandleRow.Field<double>(instrument + "_high"))) &
                                     (!Double.IsNaN(CandleRow.Field<double>(instrument + "_low"))) &
                                     (!Double.IsNaN(CandleRow.Field<double>(instrument + "_close"))))
                               select CandleRow);

            if (QueryResult.Count() < 3 * LookBack1 / 4)
            {
                return;
            }

            CandleStickTableSelected = QueryResult.CopyToDataTable();

            double HH = Convert.ToDouble(CandleStickTableSelected.Compute("Max(" + instrument + "_high" + ")", ""));
            double LL = Convert.ToDouble(CandleStickTableSelected.Compute("Min(" + instrument + "_low" + ")", ""));

            Data.Rows[StoIndex][instrument + "_hh"] = HH;
            Data.Rows[StoIndex][instrument + "_ll"] = LL;

            Data.Rows[StoIndex][instrument + "_K"] = 100 * (candleStickTable.Rows[CandleIndex].Field<double>(instrument + "_close") - LL) / (HH - LL);
            
            if (StoIndex < LookBack1 + LookBack2 - 2)
            {
                return;
            }

            DataTable StoTableSelected = Data.AsEnumerable().Skip(StoIndex - LookBack2 + 1).Take(LookBack2).CopyToDataTable();
            Data.Rows[StoIndex][instrument + "_D1"] = Convert.ToDouble(StoTableSelected.Compute("Sum(" + instrument + "_K" + ")", "").ToString()) / LookBack2;

            if (StoIndex < LookBack1 + LookBack2 + LookBack3 - 3)
            {
                return;
            }

            StoTableSelected = Data.AsEnumerable().Skip(StoIndex - LookBack3 + 1).Take(LookBack3).CopyToDataTable();
            Data.Rows[StoIndex][instrument + "_D2"] = Convert.ToDouble(StoTableSelected.Compute("Sum(" + instrument + "_D1" + ")", "").ToString()) / LookBack3;

        }
    }
}
