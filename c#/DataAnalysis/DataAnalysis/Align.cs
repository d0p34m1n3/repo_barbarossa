using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataAnalysis
{
    public static class Align
    {
        public static List<DataTable> GenerateAlignedTimeSeries(List<string>seperatorList,string seperatorField,string alignerField,DataTable dataTableInput)
        {
            List<DataTable> AlignedDataList = new List<DataTable>();
            List<DateTime> AlignedDates = null;

            int NumContracts = seperatorList.Count();

            for (int i = 0; i < NumContracts; i++)
            {

                AlignedDataList.Add(dataTableInput.Select(seperatorField + "='" + seperatorList[i] + "'").CopyToDataTable());

                if (i == 0)
                    AlignedDates = AlignedDataList[i].AsEnumerable().Select(x => x.Field<DateTime>(alignerField)).ToList();
                else
                {
                    AlignedDates = AlignedDates.Intersect(AlignedDataList[i].AsEnumerable().Select(x => x.Field<DateTime>(alignerField)).ToList()).ToList();
                }
            }

            for (int i = 0; i < NumContracts; i++)
            {
                AlignedDataList[i] = (from x in AlignedDataList[i].AsEnumerable()
                                      where AlignedDates.Any(b => x.Field<DateTime>("price_date") == b)
                                      select x).CopyToDataTable();
            }

            return AlignedDataList;
        }
    }
}
