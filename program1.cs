using System;
using System.Data;
using IndiGo.Zensar.Utility;

namespace FltPTMBTM
{
    class Program
    {
        static LogHelper logHelper = new LogHelper();

        static void Main(string[] args)
        {
            FetchFlightDetails();
        }

        public static string GetAndSetFlightNumber(string CurrFlightNumber)
        {
            string ModifiedFlightNumber = string.Empty;
            switch (CurrFlightNumber.Length)
            {
                case 0: ModifiedFlightNumber = "    "; break;
                case 1: ModifiedFlightNumber = "   " + CurrFlightNumber; break;
                case 2: ModifiedFlightNumber = "  " + CurrFlightNumber; break;
                case 3: ModifiedFlightNumber = " " + CurrFlightNumber; break;
                default: ModifiedFlightNumber = CurrFlightNumber; break;
            }
            return ModifiedFlightNumber;
        }

        public static void FetchFlightDetails()
        {
            try
            {
                Console.WriteLine("Fetch Flight Details is started at:" + DateTime.Now);
                logHelper.LogInfo("Fetch Flight Details", "Started at:" + DateTime.Now);

                // API replaced – this now comes from Manifest API instead of SP
                DataTable CollectDt = DataLayer.GetFlightDataDetails();

                foreach (DataRow DR in CollectDt.Rows)
                {
                    string CarrieerCode = (!string.IsNullOrEmpty(DR["AIRLINE"].ToString()) ? DR["AIRLINE"].ToString() : "6E");
                    string ModifiedFlightNo = GetAndSetFlightNumber(DR["FLTNBR"].ToString());
                    string OpSuffix = " ";
                    string Departure = (!string.IsNullOrEmpty(DR["DEP"].ToString()) ? DR["DEP"].ToString() : "");

                    Console.WriteLine("----------------------------------------------------------");
                    Console.WriteLine("Get And Insert PTMBTM Data is started at:" + DateTime.Now);

                    logHelper.LogInfo("Insert PTMS", 
                        "Started at:" + DateTime.Now + " " + CarrieerCode + " " + ModifiedFlightNo + " " + OpSuffix + " " + Departure);

                    // API version — this inserts BTM/PTM using API data instead of SP
                    DataLayer.GetAndInsertPTMBTMData(DateTime.Now, CarrieerCode, ModifiedFlightNo, OpSuffix, Departure);

                    logHelper.LogInfo("Insert PTM End", "Completed at:" + DateTime.Now);
                    Console.WriteLine("Get And Insert PTMBTM Data is End at:" + DateTime.Now);
                    Console.WriteLine("----------------------------------------------------------");
                }

                logHelper.LogInfo("Fetch Flight Details", "Completed at:" + DateTime.Now);
                Console.WriteLine("Fetch Flight Details is End at:" + DateTime.Now);
            }
            catch (Exception ex)
            {
                logHelper.LogError("Fetch Flight Detail Exception occured at :" + DateTime.Now,
                                   "Exception occured::" + ex.Message);
                Console.WriteLine("Fetch Flight Detail Exception occured at :" + DateTime.Now + ex.Message);
            }
        }
    }
}
