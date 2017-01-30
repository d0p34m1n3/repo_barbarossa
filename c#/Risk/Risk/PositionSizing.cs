using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Risk
{
    public static class PositionSizing
    {
        public static Dictionary<string, double> GetStrategyTargetSize()
        {
            List<string> lineList = IOUtilities.FromTxt2List.LoadFile(TA.DirectoryNames.GetDirectoryName("ttapiConfig") + TA.FileNames.BetSizeFile);
            double BetSize = Convert.ToDouble(lineList[0]);

            return new Dictionary<string, double>()
            {
                {"ButterflyFirstBetSize",BetSize * 53},
                {"ScvSize",BetSize * 53},
                {"ButterflyFinalBetSize",BetSize * 88},
                {"VcsBetSize",BetSize * 132},
                {"CarrySpreadBetSize",BetSize * 13},
                {"PcaCurveBetSize",BetSize * 20}
            };
        }
    }
}
