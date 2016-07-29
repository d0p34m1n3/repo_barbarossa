using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FormatUtilities
{
    public static class FromString2Dictionary
    {

        public static Dictionary<string,string> Convert(string stringInput)
        {
            Dictionary<string, string> dictionaryOut = new Dictionary<string, string>();

            string[] pairs = stringInput.Split('&');

            for (int i = 0; i < pairs.Count(); i++)
            {
                string[] keyAndValue = pairs[i].Split('=');
                dictionaryOut.Add(keyAndValue[0], keyAndValue[1]);
            }
            return dictionaryOut;

        }

    }
}
