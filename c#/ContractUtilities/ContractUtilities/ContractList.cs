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
        public List<TA.ttapiTicker> ttapiTickerList
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
            ttapiTickerList = new List<TA.ttapiTicker>();
            dbTickerList = new List<string>();

            for (int i = 0; i < instrumentList.Length; i++)
            {
                ContractVolumeList = TA.LoadContractVolumeFile.GetContractVolumes(settleDate).ToList();

                ttapiTickerList.Add((ttapiTicker)ContractVolumeList.Where(x => x.productName == TA.TickerheadConverters.ConvertFromDB2TT(instrumentList[i])
                    && x.productType == "FUTURE").OrderByDescending(x => x.Volume).FirstOrDefault());
                dbTickerList.Add(TA.TickerConverters.ConvertFromTTAPIFields2DB(ttapiTickerList[i].productName, ttapiTickerList[i].instrumentName));
            }
        }

        public ContractList(string[] instrumentList)
            : this(CalendarUtilities.BusinessDays.GetBusinessDayShifted(-1), instrumentList)
        {
        }
    }
}
