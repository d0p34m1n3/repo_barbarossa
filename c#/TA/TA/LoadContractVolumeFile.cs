using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CalendarUtilities;
using CsvHelper;
using CsvHelper.Configuration;
using System.IO;


namespace TA
{
    public class ContractVolume:ttapiTicker
    {
        public int Volume { get; set; }

    }

    public sealed class MyClassMap : CsvClassMap<ContractVolume>
    {
        public MyClassMap()
        {
            Map(m => m.instrumentName).Name("InstrumentName");
            Map(m => m.marketKey).Name("MarketKey");
            Map(m => m.productType).Name("ProductType");
            Map(m => m.productName).Name("ProductName");
            Map(m => m.Volume).Default(0);
        }
    }


    public static class LoadContractVolumeFile
    {
        private static List<ContractVolume> ContractVolumeList;
        private static string ContractVolumeFolder;

        public static IEnumerable<ContractVolume> GetContractVolumes()
        {
            DateTime folderDate = BusinessDays.GetBusinessDayShifted(-1);
            return GetContractVolumes(folderDate);
        }

        public static IEnumerable<ContractVolume> GetContractVolumes(DateTime folderDate)
        {
            ContractVolumeFolder = DirectoryNames.GetDirectoryName("ttapiContractVolume") + DirectoryNames.GetDirectoryExtension(BusinessDays.GetBusinessDayShifted(-1));
            var sr = new StreamReader(ContractVolumeFolder + "/ContractList.csv");
            var reader = new CsvHelper.CsvReader(sr);
            reader.Configuration.RegisterClassMap(new MyClassMap());
            ContractVolumeList = reader.GetRecords<ContractVolume>().ToList();

            for (int i = 0; i < ContractVolumeList.Count(); i++)
            {
                string[] words = ContractVolumeList[i].instrumentName.Split();
                ContractVolumeList[i].SeriesKey = words[words.Count() - 1];
            }

            return ContractVolumeList;
        }
        
       
    }
}
