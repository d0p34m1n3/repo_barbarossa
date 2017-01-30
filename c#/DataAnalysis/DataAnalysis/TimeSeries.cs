using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataAnalysis
{
    public class TimeSeries
    {
        DateTime CurrentTime;
        List<DateTime> TimeSeriesList;

        public DataTable Data
        {
            get;
            set;
        }

        public TimeSeries(DateTime startDate, DateTime endDate, string[] instrumentList, int minInterval)
        {
            CurrentTime = startDate;
            TimeSeriesList = new List<DateTime>();
           
            Data = new DataTable();
            Data.Columns.Add("TimeStamp", typeof(DateTime));


            while (CurrentTime <= endDate)
            {
                TimeSeriesList.Add(CurrentTime);
                CurrentTime = CurrentTime.AddMinutes(minInterval);
            }

            for (int i = 0; i < instrumentList.Length; i++)
            {
                Data.Columns.Add(instrumentList[i], typeof(double));
            }

            for (int i = 0; i < TimeSeriesList.Count; i++)
            {
                Data.Rows.Add();
                Data.Rows[i][0] = TimeSeriesList[i];

                for (int j = 0; j < instrumentList.Length; j++)
                {
                    Data.Rows[i][j + 1] = Double.NaN;
                }
            }
        }

        public void updateValues(double newPrice, DateTime newDateTime, string instrument)
        {
            int TimeIndex = TimeSeriesList.IndexOf(newDateTime);
            Data.Rows[TimeIndex][instrument] = newPrice;
        }



    }
}
