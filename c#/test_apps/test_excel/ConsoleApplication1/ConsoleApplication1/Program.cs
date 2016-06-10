using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Excel;
using System.Data;
using System.IO;

namespace ConsoleApplication1
{
    class ExcelDataReader
    {

        string FilePath;

        public ExcelDataReader(string filePath)
        {
            FilePath = filePath;
        }

        public DataSet LoadFile()
        {
            FileStream Stream = new FileStream(FilePath, FileMode.Open);
            IExcelDataReader ExcelReader2007 = ExcelReaderFactory.CreateOpenXmlReader(Stream);

            ExcelReader2007.IsFirstRowAsColumnNames = true;
            DataSet Result = ExcelReader2007.AsDataSet();
            ExcelReader2007.Close();
            return Result;

        }

        
            //Reading from a binary Excel file ('97-2003 format; *.xls)
            //IExcelDataReader excelReader2003 = ExcelReaderFactory.CreateBinaryReader(stream);

            //Reading from a OpenXml Excel file (2007 format; *.xlsx)
            //FileStream stream = new FileStream("C:/Research/strategy_output/futures_butterfly/2016/201606/20160606/butterflies.xlsx", FileMode.Open);


            

            

            //DataSet - The result of each spreadsheet will be created in the result.Tables


           // DataTable table = result.Tables["good"];

            //Data Reader methods

            //Console.Write(table.Rows[1][2]);

           // Console.Write(table.Rows[1]["ticker1"]);

           // for (int i = 0; i < 3; i++)
           // {
          //      for (int j = 0; j < table.Columns.Count; j++)


          //          Console.Write("\"" + table.Rows[i].ItemArray[j] + "\";");
          //      Console.WriteLine();





           // }


            //Free resources (IExcelDataReader is IDisposable)
            //excelReader2003.Close();
           
            //Console.Read();
        }
    }








}

