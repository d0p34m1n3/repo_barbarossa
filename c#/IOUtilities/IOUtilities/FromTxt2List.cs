using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IOUtilities
{
    public static class FromTxt2List
    {
        
        public static List<string> LoadFile(string filePath)
        {
            List<string> lineList = new List<string>();
            string line;
            System.IO.StreamReader file = new System.IO.StreamReader(filePath);

            while ((line = file.ReadLine()) != null)
            {
                lineList.Add(line);
            }

            file.Close();
            return lineList;
        }
    }
}
