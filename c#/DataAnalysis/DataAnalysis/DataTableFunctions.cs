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
    }
}
