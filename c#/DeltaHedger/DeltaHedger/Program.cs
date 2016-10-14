using DatabaseConnection;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DeltaHedger
{
    class Program
    {
        static void Main(string[] args)
        {

            Dictionary<string, string> dictionaryOut = TA.Credentials.GetTTAPICredentials();
            string ttUserId = dictionaryOut["username"];
            string ttPassword = dictionaryOut["password"];

            // Check that the compiler settings are compatible with the version of TT API installed
            TTAPIArchitectureCheck archCheck = new TTAPIArchitectureCheck();

            if (archCheck.validate())
            {
                Console.WriteLine("Architecture check passed.");

                // Dictates whether TT API will be started on its own thread
                bool startOnSeparateThread = false;

                if (startOnSeparateThread)
                {
                    // Start TT API on a separate thread
                    HedgeTTAPI HedgeApi = new HedgeTTAPI(ttUserId, ttPassword);
                    Thread workerThread = new Thread(HedgeApi.TTAPISubs.Start);
                    workerThread.Name = "TT API Thread";
                    workerThread.Start();

                    // Insert other code here that will run on this thread
                }
                else
                {
                    // Start the TT API on the same thread
                    using (HedgeTTAPI HedgeApi = new HedgeTTAPI(ttUserId, ttPassword))
                    {
                        HedgeApi.TTAPISubs.Start();
                    }
                }
            }

            else
            {
                Console.WriteLine("Architecture check failed.  {0}", archCheck.ErrorString);
            }
            Console.WriteLine("SUCCESS!! ");
        }
    }
}
