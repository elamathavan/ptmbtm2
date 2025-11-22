using IndiGo.Zensar.Utility;
using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;

namespace FltPTMBTM
{
    public static class DataLayer
    {
        //public static LogHelper log_Helper = new LogHelper();
        static SqlConnection conn;
        static string ODSconnectionString = ConfigurationManager.ConnectionStrings["ODSCONNECTION"].ConnectionString;
        static string RM_LIVEConnectionString = ConfigurationManager.ConnectionStrings["RM_LIVEConnection"].ConnectionString;
        static LogHelper logHelper = new LogHelper();

        static DataLayer()
        {
            logHelper.LogInfo("Contrctor Start", "Step 1:" + DateTime.Now);
            conn = new SqlConnection(ODSconnectionString);
            logHelper.LogInfo("Contrctor End", "Step 2:" + DateTime.Now);
            if (conn.State == ConnectionState.Closed)
                conn.Open();

            logHelper.LogInfo("Contrctor End", "Step 3:" + DateTime.Now);
        }

        public static void GetAndInsertPTMBTMData(DateTime dteFromdate, string CarrieerCode, string fltnbr, string OpSuffix, string Dep)
        {
            string CommandTimeOutValue = System.Configuration.ConfigurationManager.AppSettings["CommandTimeOut"];

            bool flag;
            try
            {
                //get flight data from AIMS   
                SqlCommand cmd = new SqlCommand();
                cmd.CommandTimeout = Convert.ToInt32((!string.IsNullOrEmpty(CommandTimeOutValue) ? CommandTimeOutValue : "500"));
                DataTable dtFlightData = new DataTable();
                conn = new SqlConnection(ODSconnectionString);
                conn.Open();
                cmd.Connection = conn;
                cmd.CommandText = "[IT].[GetCheckinReport_PTMBTM]";
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.Add(new SqlParameter("@departureDate", dteFromdate.ToString("yyyy-MM-dd")));
                cmd.Parameters.Add(new SqlParameter("@carrierCode", CarrieerCode));
                cmd.Parameters.Add(new SqlParameter("@flightNumber", fltnbr));
                cmd.Parameters.Add(new SqlParameter("@opSuffix", OpSuffix));
                cmd.Parameters.Add(new SqlParameter("@departureStation", Dep));

                SqlDataAdapter dp = new SqlDataAdapter(cmd);
                dp.Fill(dtFlightData);
                if (dtFlightData.Rows.Count > 0)
                {
                    SqlCommand cmd1 = new SqlCommand();
                    SqlConnection conn1 = new SqlConnection(RM_LIVEConnectionString);
                    conn1.Open();
                    cmd1.Connection = conn1;
                    cmd1.CommandText = "Delete from  FlightPTMBTMDetails where STD='" + Convert.ToDateTime(dtFlightData.Rows[0]["STD"]).ToString("yyyy-MM-dd HH:mm")
                                        + "' AND Dep ='" + dtFlightData.Rows[0]["DEPARTURESTATION"] + "' AND Fltnbr ='" + dtFlightData.Rows[0]["FLIGHTNUMBER"] + "'";
                    cmd1.ExecuteNonQuery();

                    SqlBulkCopy scb = new SqlBulkCopy(RM_LIVEConnectionString);

                    scb.DestinationTableName = "FlightPTMBTMDetails";
                    //scb.ColumnMappings.Add("AIRLINE", "AIRLINE");
                    //scb.ColumnMappings.Add("FLTNBR", "FLTNBR");
                    //scb.ColumnMappings.Add("DEP", "DEP");
                    //scb.ColumnMappings.Add("ARR", "ARR");
                    //scb.ColumnMappings.Add("STARTTIME", "STARTTIME");
                    //scb.ColumnMappings.Add("ENDTIME", "ENDTIME");
                    //scb.ColumnMappings.Add("fltdate", "FLIGHTDATE");
                    //scb.ColumnMappings.Add("fltdateLocal", "FLIGHTDATELOCAL");

                    scb.WriteToServer(dtFlightData);
                    scb.Close();
                    //Insert record from H_M_FlightInfo
                    conn1.Close();
                }
                cmd = null;
                conn.Close();
            }

            catch (Exception ex)
            {
                flag = false;
                //Log("InsertFlightDetails- " + ex.Message.ToString());
                //log_Helper.LogError("", "InsertFlightDetails- " + ex.Message.ToString());
                //LogExceptionToDB(ex, "", "InsertFlightDetails", 1);
            }
            finally
            {
                conn.Close();
            }
        }

        public static DataTable GetFlightDataDetails()
        {
            logHelper.LogInfo("GetFlightDataDetails", "Step 1:" + DateTime.Now);

            DataSet DataTableSchConfig = new DataSet();
             string Constr = ConfigurationManager.ConnectionStrings["ConnProdDb"].ConnectionString;
            string CommandTimeOutValue = System.Configuration.ConfigurationManager.AppSettings["CommandTimeOut"];
            try
            {
                using (SqlConnection sqlConn = new SqlConnection(Constr))
                {
                  //  string StrQry = " select  * from T_FlightData where Convert( varchar(10),Starttime,120)= Convert(varchar(10),GETUTCDATE(),120)";
                  //  StrQry += " and (starttime  >=Dateadd( Minute,5,GETUTCDATE()  )  and starttime  <= Dateadd( Minute,15,GETUTCDATE()  ) )";
                  ////  StrQry += " and fltnbr=232  and dep='MAA'";
                   // StrQry += " and  BLOCKOFF  is not null   ";
                    string StrQry = " select  * from T_FlightData where Convert( varchar(10),Case when ESTIMATEDBLOCKOFF is null then STARTTIME else ESTIMATEDBLOCKOFF end,120)= Convert(varchar(10),GETUTCDATE(),120)";
                    StrQry += " and (Case when ESTIMATEDBLOCKOFF is null then STARTTIME else ESTIMATEDBLOCKOFF end  >=Dateadd( Minute,5,GETUTCDATE()  )  ";
                    StrQry += "and Case when ESTIMATEDBLOCKOFF is null then STARTTIME else ESTIMATEDBLOCKOFF end  <= Dateadd( Minute,15,GETUTCDATE()  ) )";
                 
                    using (SqlCommand com = new SqlCommand(StrQry, sqlConn))
                    {
                        using (var da = new SqlDataAdapter(com))
                        {
                            com.CommandType = CommandType.Text;
                            com.CommandTimeout = Convert.ToInt32((!string.IsNullOrEmpty(CommandTimeOutValue) ? CommandTimeOutValue : "500"));
                            sqlConn.Open();
                            da.Fill(DataTableSchConfig);
                            da.Dispose();
                            sqlConn.Close();
                        }
                    }
                }
            }
            catch (Exception Ex)
            {
                throw Ex;
            }
            return DataTableSchConfig.Tables[0];
        }
    }
}
