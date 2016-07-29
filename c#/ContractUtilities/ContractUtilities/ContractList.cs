using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TA;
using CalendarUtilities;

namespace ContractUtilities
{
    public class ContractList
    {
        public List<TA.ContractVolume> ttapiTickerList
        {
            get;
            set;
        }
        public List<TA.ContractVolume> ContractVolumeList;

        public List<string> dbTickerList
        {
            get;
            set;
        }

        public ContractList(DateTime settleDate, string[] instrumentList)
        {
            ttapiTickerList = new List<TA.ContractVolume>();
            dbTickerList = new List<string>();

            for (int i = 0; i < instrumentList.Length; i++)
            {
                ContractVolumeList = TA.LoadContractVolumeFile.GetContractVolumes(settleDate).ToList();

                ttapiTickerList.Add(ContractVolumeList.Where(x => x.ProductName == TA.TickerheadConverters.ConvertFromDB2TT(instrumentList[i])
                    && x.ProductType == "FUTURE").OrderByDescending(x => x.Volume).FirstOrDefault());
                dbTickerList.Add(TA.TickerConverters.ConvertFromTTAPIFields2DB(ttapiTickerList[i].ProductName, ttapiTickerList[i].InstrumentName));
            }
        }

        public ContractList(string[] instrumentList)
            : this(CalendarUtilities.BusinessDays.GetBusinessDayShifted(-1), instrumentList)
        {
        }
    }
}
