using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;

namespace DataAnalysis
{
    public class CandleStick
    {
        DateTime currentTime;
        List<DateTime> candleStartTimeList;
        List<DateTime> candleEndTimeList;
        Double highPrice;
        Double lowPrice;
        Double openPrice;
        Double closePrice;

        public DataTable data
        {
            get;
            set;
        }

        public CandleStick(DateTime startDate, DateTime endDate, string[] instrumentList, int minInterval)
        {
            currentTime = startDate;
            candleStartTimeList = new List<DateTime>();
            candleEndTimeList = new List<DateTime>();
            data = new DataTable();
            data.Columns.Add("start", typeof(DateTime));
            data.Columns.Add("end", typeof(DateTime));

            while (currentTime.AddMinutes(minInterval) <= endDate)
            {
                candleStartTimeList.Add(currentTime);
                candleEndTimeList.Add(currentTime.AddMinutes(minInterval));
                currentTime = currentTime.AddMinutes(minInterval);
            }

            for (int i = 0; i < instrumentList.Length; i++)
            {
                data.Columns.Add(instrumentList[i] + "_high", typeof(double));
                data.Columns.Add(instrumentList[i] + "_low", typeof(double));
                data.Columns.Add(instrumentList[i] + "_open", typeof(double));
                data.Columns.Add(instrumentList[i] + "_close", typeof(double));
            }

            for (int i = 0; i < candleStartTimeList.Count; i++)
            {
                data.Rows.Add();
                data.Rows[i][0] = candleStartTimeList[i];
                data.Rows[i][1] = candleEndTimeList[i];

                for (int j = 0; j < instrumentList.Length; j++)
                {
                    data.Rows[i][4 * j + 2] = Double.NaN;
                    data.Rows[i][4 * j + 3] = Double.NaN;
                    data.Rows[i][4 * j + 4] = Double.NaN;
                    data.Rows[i][4 * j + 5] = Double.NaN;
                }
            }
        }

        public void updateValues(double newPrice,DateTime newDateTime, string instrument)

        {
            int timeIndex = Enumerable.Range(0, data.Rows.Count).Where(i => Convert.ToDateTime(data.Rows[i]["end"]) > newDateTime).ToList()[0];

            highPrice = (double)data.Rows[timeIndex][instrument + "_high"];
            lowPrice = (double)data.Rows[timeIndex][instrument + "_low"];

            openPrice = (double)data.Rows[timeIndex][instrument + "_open"];
            closePrice = (double)data.Rows[timeIndex][instrument + "_close"];

            if ((Double.IsNaN(highPrice)) || (newPrice > highPrice))
            {
                data.Rows[timeIndex][instrument + "_high"] = newPrice;
            }

            if ((Double.IsNaN(lowPrice)) || (newPrice < lowPrice))
            {
                data.Rows[timeIndex][instrument + "_low"] = newPrice;
            }

            if (Double.IsNaN(openPrice))
            {
                data.Rows[timeIndex][instrument + "_open"] = newPrice;
            }


            data.Rows[timeIndex][instrument + "_close"] = newPrice;

        }


    }
}
