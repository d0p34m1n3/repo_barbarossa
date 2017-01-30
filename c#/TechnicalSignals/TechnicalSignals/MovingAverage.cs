using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TechnicalSignals
{
    public static class MovingAverage
    {
        public static double CalculateTimeSeriesMovingAverage(DataTable timeSeries, string columnName,DateTime dateTimePoint,int numObs)
        {
            List<DateTime> TimeStampList = DataAnalysis.DataTableFunctions.GetColumnAsList<DateTime>(dataTableInput: timeSeries, columnName: "TimeStamp");
            int EndIndex = TimeStampList.IndexOf(dateTimePoint);
            DataTable TimeSeriesTableSelected = timeSeries.AsEnumerable().Skip(EndIndex - numObs+1).Take(numObs).CopyToDataTable();
            return Convert.ToDouble(TimeSeriesTableSelected.Compute("Sum(" + columnName + ")", "").ToString()) / numObs;
        }
    }
}
