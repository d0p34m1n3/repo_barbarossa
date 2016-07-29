using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.IO;
using Excel;

namespace IOUtilities
{
    public static class ExcelDataReader
    {

        public static DataSet LoadFile(string filePath)
        {
            FileStream Stream = new FileStream(filePath, FileMode.Open);
            IExcelDataReader ExcelReader2007 = ExcelReaderFactory.CreateOpenXmlReader(Stream);

            ExcelReader2007.IsFirstRowAsColumnNames = true;
            DataSet Result = ExcelReader2007.AsDataSet();
            ExcelReader2007.Close();
            return Result;
        
        }
    }
}


