using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Forms;
using TradingTechnologies.TTAPI;

namespace WindowsFormsApplication1
{
    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        static void Main()
        {
            
            using (var disp = Dispatcher.AttachUIDispatcher())
            {
                Application.EnableVisualStyles();
                Application.SetCompatibleTextRenderingDefault(false);

                Form1 form1 = new Form1();
                ApiInitializeHandler handler = new ApiInitializeHandler(form1.ttApiInitHandler);
                TTAPI.CreateUniversalLoginTTAPI(disp, "ekocatulum","rubicon1789",handler);

                Application.Run(form1);

            }



        }
    }
}
