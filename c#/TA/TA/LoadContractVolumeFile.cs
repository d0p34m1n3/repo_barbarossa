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
    public class ContractVolume
    {
        public String InstrumentName { get; set;}
        public String MarketKey { get; set; }
        public String ProductType { get; set; }
        public String ProductName { get; set; }
        public int Volume { get; set; }

    }

    public sealed class MyClassMap : CsvClassMap<ContractVolume>
    {
        public MyClassMap()
        {
            Map(m => m.InstrumentName);
            Map(m => m.MarketKey);
            Map(m => m.ProductType);
            Map(m => m.ProductName);
            Map(m => m.Volume).Default(0);
        }
    }


    public static class LoadContractVolumeFile
    {
        private static IEnumerable<ContractVolume> ContractVolumeList;
        private static string ContractVolumeFolder;

        public static IEnumerable<ContractVolume> GetContractVolumes()
        {
            DateTime folderDate = BusinessDays.GetBusinessDayShifted(-1);
            return GetContractVolumes(folderDate);
        }

        public static IEnumerable<ContractVolume> GetContractVolumes(DateTime folderDate)
        {
            ContractVolumeFolder = DirectoryNames.ttapiContractVolumeDirectory + BusinessDays.GetDirectoryExtension(BusinessDays.GetBusinessDayShifted(-1));
            var sr = new StreamReader(ContractVolumeFolder + "/ContractList.csv");
            var reader = new CsvHelper.CsvReader(sr);
            reader.Configuration.RegisterClassMap(new MyClassMap());
            ContractVolumeList = reader.GetRecords<ContractVolume>();
            return ContractVolumeList;
        }
        
       
    }
}
