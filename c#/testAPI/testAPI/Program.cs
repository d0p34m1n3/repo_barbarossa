using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using ContractUtilities;

namespace testAPI
{
    using TradingTechnologies.TTAPI;
    class Program
    {
        


        static void Main(string[] args)
        {
            string ttUserId = "ekocatulum";
            string ttPassword = "pompei1789";

            apiInitialize tf = new apiInitialize(ttUserId, ttPassword);

            tf.TickerheadList = ContractMetaInfo.FuturesButterflyTickerheadList;

            Thread workerThread = new Thread(tf.Start);
            workerThread.Name = "TT API Thread";
            workerThread.Start();

            Console.ReadLine();


        }

        
        }
    }

