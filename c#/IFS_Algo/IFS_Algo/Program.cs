using ContractUtilities;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TradingTechnologies.TTAPI;

namespace IFS_Algo
{
    class Program
    {
        static void Main(string[] args)
        {
            Dictionary<string, string> dictionaryOut = TA.Credentials.GetTTAPICredentials();
            string ttUserId = dictionaryOut["username"];
            string ttPassword = dictionaryOut["password"];

            TTAPIArchitectureCheck archCheck = new TTAPIArchitectureCheck();

            if (archCheck.validate())
            {
                Console.WriteLine("Architecture check passed.");

                // Dictates whether TT API will be started on its own thread
                bool startOnSeparateThread = false;

                if (startOnSeparateThread)
                {
                    // Start TT API on a separate thread
                    Algo SpreadAlgo = new Algo(ttUserId, ttPassword);
                    
                    Thread workerThread = new Thread(SpreadAlgo.TTAPISubs.Start);
                    workerThread.Name = "TT API Thread";
                    workerThread.Start();

                    // Insert other code here that will run on this thread
                }
                else
                {
                    // Start the TT API on the same thread
                    using (Algo SpreadAlgo = new Algo(ttUserId, ttPassword))
                    {
                        //sv.TickerheadList = ContractMetaInfo.FuturesButterflyTickerheadList;

                        SpreadAlgo.TTAPISubs.Start();
                    }
                }
            }

            else
            {
                Console.WriteLine("Architecture check failed.  {0}", archCheck.ErrorString);
            }
            Console.WriteLine("SUCCESS!! ");
            

            string emre = "emre";

        }
    }
}
