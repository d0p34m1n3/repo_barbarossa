using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;

namespace DataAnalysis
{
    public static class DataTableFunctions
    {
        public static void CleanNullRows(DataTable dataTableInput,string[] columnNames)
        {
            for (int i = dataTableInput.Rows.Count - 1; i >= 0; i--)
            {
                for (int j = 0; j < columnNames.Length; j++)
                {
                    if (dataTableInput.Rows[i][columnNames[j]] == DBNull.Value)
                    { 
                        dataTableInput.Rows[i].Delete();
                        break;
                    }
                }
            }
            dataTableInput.AcceptChanges();

        }

        public static List<T> GetColumnAsList<T>(DataTable dataTableInput, string columnName,bool uniqueQ=false)
        {
            if (uniqueQ)
            {
                return dataTableInput.AsEnumerable().Select(s => s.Field<T>(columnName)).Distinct().ToList();
            }
            else
            {
                return dataTableInput.AsEnumerable().Select(s => s.Field<T>(columnName)).ToList();
            }
        }

        public static DataTable Sort(DataTable dataTableInput, string[] columnList, string[] orderList=null)
        {
            // Order: ASC or DESC
            DataView Dv = dataTableInput.DefaultView;

            string SortString = "";
 
            for (int i = 0; i < columnList.Length; i++)
            {
                string CommmaString;
                if (i==0)
                {
                    CommmaString = "";
                }
                else
                {
                    CommmaString = ", ";
                }


                if ((orderList == null || orderList.Length == 0))
                {
                    SortString = SortString + CommmaString + columnList[i] + " ASC";
                }
                else
                {
                    SortString = SortString + CommmaString + columnList[i] + " "+ orderList[i];
                }
            }
            Console.WriteLine(SortString);
            Dv.Sort = SortString;
            //   Convert back your sorted DataView to DataTable
            return Dv.ToTable();
        }

    }
}
