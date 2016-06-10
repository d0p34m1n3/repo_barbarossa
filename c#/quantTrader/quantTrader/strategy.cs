using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace quantTrader
{
    class Strategy
    {
        List<Instrument> Instruments { get; set; }
        List<Decimal> Quantities { get; set; }
        string StrategyAlias { get; set; }
        string StrategyClass { get; set; }

        public Strategy(string strategyAlias, string strategyClass)
        {
            StrategyAlias = strategyAlias;
            StrategyClass = StrategyClass;
        }
    }
}
