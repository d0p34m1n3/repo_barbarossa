using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using IOUtilities;
using FormatUtilities;

namespace TA
{
    public static class Credentials
    {
        public static Dictionary<string,string> GetTTAPICredentials()
        {
            List<string> lineList = IOUtilities.FromTxt2List.LoadFile(DirectoryNames.ttapiConfigDirectory + FileNames.ttapi_credential_file);
            return FormatUtilities.FromString2Dictionary.Convert(lineList[0]);
        }

    }
}
