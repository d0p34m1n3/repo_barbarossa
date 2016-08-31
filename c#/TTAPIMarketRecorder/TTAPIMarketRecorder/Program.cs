using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using TA;

namespace TTAPIMarketRecorder
{
    using TradingTechnologies.TTAPI;
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
                    MarketRecorder mr = new MarketRecorder(ttUserId, ttPassword);
                    Thread workerThread = new Thread(mr.TTAPISubs.Start);
                    workerThread.Name = "TT API Thread";
                    workerThread.Start();

                    // Insert other code here that will run on this thread
                }
                else
                {
                    // Start the TT API on the same thread
                    using (MarketRecorder mr = new MarketRecorder(ttUserId, ttPassword))
                    {
                        mr.TTAPISubs.Start();
                    }
                }
            }

            else
            {
                Console.WriteLine("Architecture check failed.  {0}", archCheck.ErrorString);
            }
        }
    }
}
