using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Shared
{
    public static class Converters
    {
        public static Dictionary<string,string> ConvertFromStringToDictionary(string stringInput)
        {
            return stringInput.Split('&').Select(x => x.Split('=')).ToDictionary(x => x[0], x => x[1]);
        }
    }
}
