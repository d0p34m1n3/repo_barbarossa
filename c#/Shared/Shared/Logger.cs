using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Shared
{
    public class Logger
    {
        public StreamWriter SW;
        public bool consolePrintQ { set; get;}
        public bool includeTimeQ { set; get; }

        public Logger(StreamWriter sw)
        {
            SW = sw;
            consolePrintQ = true;
            includeTimeQ = true;
        }

        public void Log(string text)
        {
            if (includeTimeQ)
            {
                text = DateTime.Now.ToString("HH:mm:ss") + " " +  text;
            }

            SW.WriteLine(text);
            SW.Flush();

            if (consolePrintQ)
            {
                Console.WriteLine(text);
            }
        }
    }
}
