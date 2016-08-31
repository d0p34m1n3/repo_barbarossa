using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using ContractUtilities;
using TA;

namespace SaveVolume
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
                    saveVolume sv = new saveVolume(ttUserId, ttPassword);
                    sv.TickerheadList = ContractMetaInfo.cmeFuturesTickerheadList.Union(ContractMetaInfo.iceFuturesTickerheadList).ToList();
                    Thread workerThread = new Thread(sv.TTAPISubs.Start);
                    workerThread.Name = "TT API Thread";
                    workerThread.Start();

                    // Insert other code here that will run on this thread
                }
                else
                {
                    // Start the TT API on the same thread
                    using (saveVolume sv = new saveVolume(ttUserId, ttPassword))
                    {
                        //sv.TickerheadList = ContractMetaInfo.FuturesButterflyTickerheadList;
                        sv.TTAPISubs.TickerHeadList = ContractMetaInfo.cmeFuturesTickerheadList.Union(ContractMetaInfo.iceFuturesTickerheadList).ToList();


                        sv.TTAPISubs.Start();
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
