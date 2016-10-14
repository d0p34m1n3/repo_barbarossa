using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StrategyUtilities
{
    public class PositionManagerOutput
    {
        public DataTable SortedPosition { set; get; }
        public double Scale { set; get; }
        public bool CorrectPositionQ { set; get; }
    }

    public static class SpreadDefinitions
    {
        public static Dictionary<string, List<decimal>> WeightDictionary = new Dictionary<string, List<decimal>> { 
        { "HO_CL", new List<decimal> { 1, -1 } },
        { "RB_CL", new List<decimal> { 1, -1 } },
        { "CL_B", new List<decimal> { 1, -1 } },
        { "S_BO_SM", new List<decimal> { 1, -1,-1 } },
        { "C_W", new List<decimal> { 1, -1 } },
        { "W_KW", new List<decimal> { 1, -1 } }};

        public static Dictionary<string, List<double>> AutoSpreaderMultiplierDictionary = new Dictionary<string, List<double>> { 
        { "HO_CL", new List<double> { 0.42, -1 } },
        { "RB_CL", new List<double> { 0.42, -1 } },
        { "CL_B", new List<double> { 1, -100 } },
        { "S_BO_SM", new List<double> { 100, -0.11, -0.22 } },
        { "C_W", new List<double> { 1, -1 } },
        { "W_KW", new List<double> { 1, -1 } }};
    }
}
