using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using ClosedXML;

namespace IOUtilities
{
    public static class ExcelDataWriter
    {
        public static void WriteDataTable(DataTable Data2Write,string FileName,string SheetName)
        {
            ClosedXML.Excel.XLWorkbook wb = new ClosedXML.Excel.XLWorkbook();
            wb.Worksheets.Add(Data2Write, SheetName);
            wb.SaveAs(FileName);
        }
    }
}
