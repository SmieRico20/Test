using System;
using System.Linq;
using System.Web;
using System.Xml.Linq;
using System.Net;
using System.IO;
using System.Text;
using MySql.Data.MySqlClient;
using System.Data;
using log4net;
using log4net.Config;

//namespace InsertTransaction
//{

    public class InsertPOTransaction
    {

        //private String Server;
        //private String Database;
        //private String User;
        //private String Pass;
        //private String pool;
        //private String minpool;
        //private String maxpool;
        //private String timeout;
        ////private String constring;
        //private DBConnects dbconPartner;
        public MySqlCommand command;
        public MySqlTransaction trans;
        private static readonly ILog kplog = LogManager.GetLogger(typeof(InsertPOTransaction));
        public MySqlConnection con;
        public String SendOutControl;
        //ConnectDB();
        //public void ConnectDB()
        //{ 
        //    //String strpath = "C:\\kpconfig\\DBPartner.ini";
        //    //IniFiles ini = new IniFiles(strpath);
        //    //Server = ini.IniReadValue("DBConfig Partner", "Server");
        //    //Database = ini.IniReadValue("DBConfig Partner", "Database");
        //    //User = ini.IniReadValue("DBConfig Partner", "UID");
        //    //Pass = ini.IniReadValue("DBConfig Partner", "Password");
        //    //pool = ini.IniReadValue("DBConfig Partner", "Pool");
        //    //maxpool = ini.IniReadValue("DBConfig Partner", "MaxCon");
        //    //minpool = ini.IniReadValue("DBConfig Partner", "MinCon");
        //    //timeout = ini.IniReadValue("DBConfig Partner", "Tout");


        //    //Server = "192.168.12.215";
        //    //Database = "kpadminpartners";
        //    //User = "kp6usr";
        //    //Pass = "143MLINCkp6";
        //    //pool = "1";
        //    //maxpool = "100";
        //    //minpool = "0";
        //    //timeout = "60";

        //    int maxpol = Convert.ToInt32(maxpool);
        //    int minpol = Convert.ToInt32(minpool);
        //    int to = Convert.ToInt32(timeout);
        //    dbconPartner = new DBConnects(Server, Database, User, Pass, pool, maxpol, minpol, to);
        //}
        public String SavePayOutTrans(int transtype, String ReceiptNo, String ReferenceNo, String OperatorID, String StationID, String IRNo, int IsRemote, String RemoteBranch, String RemoteOperatorID, String Reason, String AccountCode, String Currency, Decimal Principal, Decimal Forex, String Relation, String IDType, String IDNo, String ExpiryDate, String BranchCode, int ZoneCode, String CancelledDate, String CancelledByOperatorID, String CancelledByStationID, String CancelledType, String CancelledReason, Decimal CancelledCustCharge, String CancelledByBranchCode, String ReceiverFName, String ReceiverLName, String ReceiverMName, String ReceiverName, String RecevierAddress, String ReceiverGender, String ReceiverContactNo, String ReceiverBirthDate, Decimal CancelledEmpCharge, Decimal Balance, Decimal DormantCharge, Decimal ServiceCharge, int RemoteZoneCode, String SenderFName, String SenderLName, String SenderMName, String SenderName, int CancelledByZoneCode, String SenderAddress, String SenderContactNo, String SenderGender, String TraceNo, String SessionID, String ReceiverStreet, String ReceiverProvince, String ReceiverCountry, String KPTN, String ControlNum)
        {
            try
            {

                if (CheckRefNoPO(ReferenceNo, AccountCode))
                {
                    return "-1";
                }
               
                //con.Open();
                        //using (command = con.CreateCommand())
                        //{
                            MySqlCommand command = con.CreateCommand();
                            //String ControlNum = generateControlGlobal(BranchCode, transtype, OperatorID, ZoneCode, StationID, 1.2, "");
                            //if (ControlNum.Contains("Error"))
                            //{
                            //    //return ControlNum.ToString();
                            //    //con.Close();
                            //    return "-1";
                            //}
                            String payoutdate = getServerDatePartner(true).ToString("yyyy-MM-dd hh:mm:ss");
                            String pdate = getServerDatePartner(true).ToString("MM-dd").Replace("-", "");
                           
                            //trans = con.BeginTransaction(IsolationLevel.ReadCommitted);
                            //command.Transaction = trans;

                            command.CommandText = "Insert into kppartners.payout" + pdate + "(ControlNo, ReferenceNo, ClaimedDate, OperatorID, " +
                                                  "StationID, IRNo, IsRemote, RemoteBranch, RemoteOperatorID, Reason, AccountCode, Currency, " +
                                                  "Principal, Forex, Relation, IDType, IDNo, ExpiryDate, BranchCode, ZoneCode, CancelledDate," +
                                                  "CancelledByOperatorID,CancelledByStationID, CancelledType, CancelledReason, CancelledCustCharge, CancelledByBranchCode, " +
                                                  "ReceiverFName, ReceiverLName, ReceiverMName, ReceiverName, ReceiverAddress, ReceiverGender, " +
                                                  "ReceiverContactNo, ReceiverBirthdate, CancelledEmpCharge, Balance, DormantCharge, ServiceCharge, " +
                                                  "RemoteZoneCode, SenderFName, SenderLName, SenderMName, SenderName, CancelledByZoneCode, SenderAddress, " +
                                                  "SenderContactNo, SenderGender, Traceno, SessionID, ReceiverStreet, ReceiverProvince, ReceiverCountry, KPTN)" +
                                                  "values('" + ControlNum + "','" + ReferenceNo + "',now(), '" + OperatorID + "','" + StationID + "','" + IRNo + "','" + IsRemote + "','" + RemoteBranch + "', " +
                                                  "'" + RemoteOperatorID + "','" + Reason + "','" + AccountCode + "','" + Currency + "','" + Principal + "','"  + Forex + "','" + Relation + "','" + IDType + "','" + IDNo + "', " +
                                                  "'" + ExpiryDate + "','" + BranchCode + "','" + ZoneCode + "','" + CancelledDate + "','" + CancelledByOperatorID + "','" + CancelledByStationID + "','" + CancelledType +"', " +
                                                  "'" + CancelledReason + "','" + CancelledCustCharge + "','" + CancelledByBranchCode + "','" + ReceiverFName + "','" + ReceiverLName + "', " +
                                                  "'" + ReceiverMName + "','" + ReceiverName + "','" + RecevierAddress + "','" + ReceiverGender + "','" + ReceiverContactNo + "','" + ReceiverBirthDate + "', " +
                                                  "'" + CancelledEmpCharge + "','" + Balance + "','" + DormantCharge + "','" + ServiceCharge + "','" + RemoteZoneCode + "','" + SenderFName + "','" + SenderLName + "'," +
                                                  "'" + SenderMName + "','" + SenderName + "','" + CancelledByZoneCode + "','" + SenderAddress + "','" + SenderContactNo + "','" + SenderGender + "'," +
                                                  "'" + TraceNo + "','" + SessionID + "','" + ReceiverStreet + "','" + ReceiverProvince + "','" + ReceiverCountry + "','" + KPTN + "');";
                            //command.Parameters.AddWithValue("ControlNum", ControlNum);
                            //command.Parameters.AddWithValue("RefNum", ReferenceNo);
                            //command.Parameters.AddWithValue("OperatorID", OperatorID);
                            //command.Parameters.AddWithValue("StationID", StationID);
                            //command.Parameters.AddWithValue("IRNum", IRNo);
                            //command.Parameters.AddWithValue("IsRemote", IsRemote);
                            //command.Parameters.AddWithValue("RemoteBranch", RemoteBranch);
                            //command.Parameters.AddWithValue("RemoteOperatorID", RemoteOperatorID);
                            //command.Parameters.AddWithValue("Reason", Reason);
                            //command.Parameters.AddWithValue("AccountCode", AccountCode);
                            //command.Parameters.AddWithValue("Currency", Currency);
                            //command.Parameters.AddWithValue("Principal", Principal);
                            //command.Parameters.AddWithValue("Forex", Forex);
                            //command.Parameters.AddWithValue("Relation", Relation);
                            //command.Parameters.AddWithValue("IDType", IDType);
                            //command.Parameters.AddWithValue("IDNo", IDNo);
                            //command.Parameters.AddWithValue("ExpiryDate", ExpiryDate);
                            //command.Parameters.AddWithValue("BranchCode", BranchCode);
                            //command.Parameters.AddWithValue("ZoneCode", ZoneCode);
                            //command.Parameters.AddWithValue("CancelledDate", CancelledDate);
                            //command.Parameters.AddWithValue("CancelledByOperatorID", CancelledByOperatorID);
                            //command.Parameters.AddWithValue("CancelledByStationID", CancelledByStationID);
                            //command.Parameters.AddWithValue("CancelledType", CancelledType);
                            //command.Parameters.AddWithValue("CancelledReason", CancelledReason);
                            //command.Parameters.AddWithValue("CancelledCustCharge", CancelledCustCharge);
                            //command.Parameters.AddWithValue("CancelledByBranchCode", CancelledByBranchCode);
                            //command.Parameters.AddWithValue("ReceiverFName", ReceiverFName);
                            //command.Parameters.AddWithValue("ReceiverLName", ReceiverLName);
                            //command.Parameters.AddWithValue("ReceiverMName", ReceiverMName);
                            //command.Parameters.AddWithValue("ReceiverName", ReceiverName);
                            //command.Parameters.AddWithValue("ReceiverAddress", RecevierAddress);
                            //command.Parameters.AddWithValue("ReceiverGender", ReceiverGender);
                            //command.Parameters.AddWithValue("ReceiverContactNo", ReceiverContactNo);
                            //command.Parameters.AddWithValue("ReceiverBirthDate", ReceiverBirthDate);
                            //command.Parameters.AddWithValue("CancelledEmpCharge", CancelledEmpCharge);
                            //command.Parameters.AddWithValue("Balance", Balance);
                            //command.Parameters.AddWithValue("Dormantcharge", DormantCharge);
                            //command.Parameters.AddWithValue("ServiceCharge", ServiceCharge);
                            //command.Parameters.AddWithValue("RemoteZoneCode", RemoteZoneCode);
                            //command.Parameters.AddWithValue("SenderFName", SenderFName);
                            //command.Parameters.AddWithValue("SenderLName", SenderLName);
                            //command.Parameters.AddWithValue("SenderMName", SenderMName);
                            //command.Parameters.AddWithValue("SenderName", SenderName);
                            //command.Parameters.AddWithValue("CancelledByZoneCode", CancelledByZoneCode);
                            //command.Parameters.AddWithValue("SenderAddress", SenderAddress);
                            //command.Parameters.AddWithValue("SenderContactNo", SenderContactNo);
                            //command.Parameters.AddWithValue("SenderGender", SenderGender);
                            //command.Parameters.AddWithValue("TraceNum", TraceNo);
                            //command.Parameters.AddWithValue("SessionID", SessionID);
                            //command.Parameters.AddWithValue("ReceiverStreet", ReceiverStreet);
                            //command.Parameters.AddWithValue("ReceiverProvince", ReceiverProvince);
                            //command.Parameters.AddWithValue("ReceiverCountry", ReceiverCountry);
                            //command.Parameters.AddWithValue("KPTN", KPTN);


                            Int32 result = command.ExecuteNonQuery();
                            if (result == 1)
                                return "1";
                            else return "-1";
                            //if (!result.Equals("1"))
                            //{
                            //    //trans.Rollback();
                            //    //con.Close();
                            //    return "-1";
                            //}
                            //else
                            //{
                            //    String ResultRef = InsertPayOutTransRef(ReferenceNo, CancelledDate, AccountCode);
                            //    if (!ResultRef.Equals("1"))
                            //    {
                            //        //trans.Rollback();
                            //        //con.Close();
                            //        return "-1";
                            //    }
                            //    else
                            //    {
                            //        //trans.Commit();
                            //        //con.Close();
                            //        return "1";
                            //    }
                            //    //trans.Commit();
                            //    //con.Close();
                            //    //return "1";
                            //}
                        //}
                        //}
            }
            catch (Exception ex)
            {
                //return ex.Message;
                return "-1";
            }

        }
        public String generateControlGlobal(String branchcode, Int32 type, String OperatorID, Int32 ZoneCode, String StationNumber, Double version, String stationcode)
        {
            //if (StationNumber.ToString().Equals("0"))
            //{
            //    kplog.Error(getRespMessage(13));
            //    return new ControlResponse { respcode = 13, message = getRespMessage(13) };
            //}

            //if (!compareVersions(getVersion(stationcode), version))
            //{
            //    return new ControlResponse { respcode = 10, message = getRespMessage(10) };
            //}
            String controlNumber = "";
            String controlno = "";
            try
            {

                //using (MySqlConnection conn = dbconPartner.getConnection())
                //{
                    //using (command = con.CreateCommand())
                    //{
                        //conn.Open();
                        //trans = con.BeginTransaction(IsolationLevel.ReadCommitted);
                        //command.Transaction = trans;
                        try
                        {
                            DateTime dt = getServerDatePartner(true);

                            String control = "";

                            //command.CommandText = "Select station, bcode, userid, nseries, zcode, type from kpformsglobal.control where station = @st and bcode = @bcode and zcode = @zcode and type = @tp FOR UPDATE";
                            command.CommandText = "Select station, bcode, userid, nseries, zcode, type from kpadminpartners.control " +
                                "where station = '" + StationNumber + "' and bcode = '" + branchcode + "' and zcode = '" + ZoneCode + "' and `type` = '" + type + "';";
                            //command.Parameters.Clear();
                            //command.Parameters.AddWithValue("st", StationNumber);
                            //command.Parameters.AddWithValue("bcode", branchcode);
                            //command.Parameters.AddWithValue("zcode", ZoneCode);
                            //command.Parameters.AddWithValue("tp", type);
                            MySqlDataReader Reader = command.ExecuteReader();

                            if (Reader.HasRows)
                            {
                                //throw new Exception("Invalid type value");
                                Reader.Read();
                                //throw new Exception(Reader["station"].ToString() + " " + Reader["bcode"].ToString() + " " + Reader["type"].ToString());
                                if (type == 0)
                                {
                                    control += "S0" + ZoneCode.ToString() + "-" + StationNumber + "-" + branchcode;
                                }
                                else if (type == 1)
                                {
                                    control += "P0" + ZoneCode.ToString() + "-" + StationNumber + "-" + branchcode;
                                }
                                else if (type == 2)
                                {
                                    control += "S0" + ZoneCode.ToString() + "-" + StationNumber + "-R" + branchcode;
                                }
                                else if (type == 3)
                                {
                                    control += "P0" + ZoneCode.ToString() + "-" + StationNumber + "-R" + branchcode;
                                }
                                else
                                {
                                    kplog.Error("Invalid type value");
                                    throw new Exception("Invalid type value");
                                }

                                String s = Reader["Station"].ToString();
                                String nseries = Reader["nseries"].ToString().PadLeft(6, '0');
                                Int32 seriesno = Convert.ToInt32(nseries) + 1;
                                nseries = seriesno.ToString().PadLeft(6, '0');
                                Reader.Close();

                                if (isSameYear2(dt))
                                {
                                    controlno = control + "-" + dt.ToString("yy") + "-" + nseries;
                                }
                                else
                                {
                                    controlno = control + "-" + dt.ToString("yy") + "-" + "000001";
                                }
                                command.Parameters.Clear();
                                command.CommandText = "Update kpadminpartners.control set nseries='" + seriesno + "' where " +
                                                      "station = '" + StationNumber + "' and bcode = '" + branchcode + "' and zcode = '" + ZoneCode + "' and `type` = '" + type + "';";
                                //command.Parameters.AddWithValue("stu", StationNumber);
                                //command.Parameters.AddWithValue("bcodeu", branchcode);
                                //command.Parameters.AddWithValue("zcodeu", ZoneCode);
                                //command.Parameters.AddWithValue("tpu", type);
                                command.ExecuteNonQuery();

                                //trans.Commit();
                                //con.Close();

                            }
                            else
                            {
                                Reader.Close();
                                command.CommandText = "Insert into kpadminpartners.control (`station`,`bcode`,`userid`,`nseries`,`zcode`, `type`) values ('" + StationNumber + "','" + branchcode + "','" + OperatorID + "',1,'" + ZoneCode + "','" + type + "')";
                                if (type == 0)
                                {
                                    control += "S0" + ZoneCode.ToString() + "-" + StationNumber + "-" + branchcode;
                                }
                                else if (type == 1)
                                {
                                    control += "P0" + ZoneCode.ToString() + "-" + StationNumber + "-" + branchcode;
                                }
                                else if (type == 2)
                                {
                                    control += "S0" + ZoneCode.ToString() + "-" + StationNumber + "-R" + branchcode;
                                }
                                else if (type == 3)
                                {
                                    control += "P0" + ZoneCode.ToString() + "-" + StationNumber + "-R" + branchcode;
                                }
                                else
                                {
                                    kplog.Error("Invalid type value");
                                    throw new Exception("Invalid type value");
                                }
                                //command.Parameters.Clear();
                                //command.Parameters.AddWithValue("station", StationNumber);
                                //command.Parameters.AddWithValue("branchcode", branchcode);
                                //command.Parameters.AddWithValue("uid", OperatorID);
                                //command.Parameters.AddWithValue("zonecode", ZoneCode);
                                //command.Parameters.AddWithValue("type", type);
                                int x = command.ExecuteNonQuery();
                                //if (x < 1) {
                                //    conn.Close();
                                //    throw new Exception("asdfsadfds");
                                //}
                                //trans.Commit();
                                //con.Close();

                                controlno = control + "-" + dt.ToString("yy") + "-" + "000001";
                            }
                        }
                        catch (MySqlException ex)
                        {
                            //trans.Rollback();
                            //con.Close();
                            //return ex.Message.ToString();
                            return "Error:" + ex.Message.ToString();
                        }
                    //}
                //}

            }
            catch (MySqlException ex)
            {
                kplog.Fatal(ex.ToString());
                //dbconPartner.CloseConnection();
                return controlNumber = "Error:" + ex.Message.ToString();
                //return new ControlResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
            }

            catch (Exception ex)
            {
                kplog.Fatal(ex.ToString());
                //dbconPartner.CloseConnection();
                //return new ControlResponse { respcode = 0, message = getRespMessage(0), ErrorDetail = ex.ToString() };
            }
            return controlNumber = controlno;
        }
        private Boolean isSameYear2(DateTime date)
        {
            try
            {
                //throw new Exception(date.Year.ToString());
                if (GetYesterday2(date).Year.Equals(date.Year))
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            catch (Exception ex)
            {
                kplog.Fatal(ex.ToString());
                throw new Exception(ex.ToString());
            }
        }
        private DateTime GetYesterday2(DateTime date)
        {
            return date.AddDays(-1);
        }
        private Boolean CheckRefNoPO(String RefNo, String AccountCode)
        {
            Boolean res = false;
            MySqlDataReader drread;
            //using (MySqlConnection cons = con)
            //{
            //    cons.Open();
                using (MySqlCommand com = con.CreateCommand())
                {
                    //String dates = getServerDatePartner(true).ToString("MM-dd").Replace("-", "");
                    com.CommandText = "Select ReferenceNo, AccountCode from kppartners.potxnref " +
                        "where ReferenceNo ='" + RefNo + "' and AccountCode = '" + AccountCode + "' and Date(CancelledDate)= '0000-00-00';";
                    drread = com.ExecuteReader();
                    if (drread.HasRows)
                    {
                        drread.Read();
                        String RefNums = drread["ReferenceNo"].ToString();
                        if (!RefNums.Equals(""))
                            res = true;
                        else res = false;
                    }
                    drread.Close();
                }
            //    cons.Close();
            //}
            return res;

        }
        private Boolean CheckRefNoSO(String RefNo, String AccountCode)
        {
            Boolean res = false;
            
            //using (MySqlConnection cons = con)
            //{
            //    cons.Open();
                using (MySqlCommand com = con.CreateCommand())
                {
                    //String dates = getServerDatePartner(true).ToString("MM-dd").Replace("-","");
                    com.CommandText = "Select ReferenceNo, AccountCode, tablereference, cancelleddate from kppartners.sotxnref " +
                        "where ReferenceNo ='" + RefNo + "' and AccountCode = '" + AccountCode + "' and Date(CancelledDate)<> '0000-00-00';";

                    MySqlDataReader drread;

                    drread = com.ExecuteReader();
                    if (drread.HasRows)
                    {

                        drread.Read();
                        String RefNums = drread["ReferenceNo"].ToString();
                        String tableref = drread["tablereference"].ToString();
                        DateTime dates = Convert.ToDateTime(drread["cancelleddate"]);
                        drread.Close();
                        if (!RefNums.Equals(""))
                        {
                            String queryme = "Update kppartners." + tableref + " set cancelleddate='" + dates.ToString("yyyy-MM-dd hh:mm:ss") + "' where ReferenceNo ='" + RefNo + "' and AccountCode = '" + AccountCode + "';";
                            com.CommandText = queryme;
                            int ins = com.ExecuteNonQuery();
                            if (ins > 0)
                                res = false;
                            else
                                res = true;
                        }  
                        else res = false;
                    }
                    drread.Close();
                   
                }
            //    cons.Close();
            //}
            return res;

        }
        public String SaveSendOutTrans(int transtype, int ZoneCode, String BranchCode, String StationID, String OperatorID, String ReferenceNo, String IRNumber, String Currency, Decimal Principal, Decimal Charge, Decimal OtherCharge, Decimal Total, String CancelledDate, String AccountCode, String CancelledByOperatorID, String CancelledByBranchCode, String CancelledByZoneCode, String CancelledByStationID, String CancelReason, String CancelDetails, String SenderFName, String SenderLName, String SenderMName, String SenderName, String ReceiverFName, String ReceiverLName, String ReceiverMName, String ReceiverName, String ReceiverAddress, String ReciverGender, String ReceiverContactNo, String ReceiverBdate, Decimal CancelCharge, String ChargeTo, Decimal Forex, String TraceNumber, String SenderAddress, String SenderGender, String SenderContactNo, String SessionID, String OtherDetails, String ReceiverStreet, String ReceiverProvince, String ReceiverCountry, String KPTN, String Message, Decimal Redeem, String SenderBirthdate)
        {
            String passhere = "0";
            String cont = "";
            try
            {


                if (CheckRefNoSO(ReferenceNo, AccountCode))
                {
                    return "-1";
                }
               
                //command.Transaction = trans;
                //ConnectDB();

                
                //if (ControlNumber.Contains("Error"))
                //{
                //    return ControlNumber.ToString();
                //}
                //else
                //{
                    //using (con = dbconPartner.getConnection())
                    //{
                    //    con.Open();
                         SendOutControl = "";
                        MySqlCommand command = con.CreateCommand();
                        String ControlNumber = generateControlGlobal(BranchCode, transtype, OperatorID, ZoneCode, StationID, 1.2, "");
                        if (ControlNumber.Contains("Error"))
                        {
                            //return ControlNumber.ToString();
                            //con.Close();
                            return "-1";
                        }
                    
                        //else
                        //{
                        //using (command = con.CreateCommand())
                        //{
                        passhere = "234";
                        cont = ControlNumber;
                            String pdate = getServerDatePartner(true).ToString("MM-dd").Replace("-", "");

                            //trans = con.BeginTransaction(IsolationLevel.ReadCommitted);
                            //command.Transaction = trans;

                            command.CommandText = "Insert into kppartners.sendout" + pdate + " (ControlNo, ReferenceNo, IRNo,Currency, Principal," +
                                                  "Charge, OtherCharge, Total, CancelledDate, AccountCode, TransDate, CancelledByOperatorID," +
                                                  "CancelledByBranchCode, CancelledByZoneCode, CancelledByStationID, CancelReason, CancelDetails, " +
                                                  "SenderFName, SenderLName, SenderMName, SenderName, ReceiverFName, ReceiverLName, ReceiverMName, " +
                                                  "ReceiverName, ReceiverAddress, ReceiverGender, ReceiverContactNo, ReceiverBirthDate, CancelCharge, " +
                                                  "ChargeTo, Forex, Traceno, SenderAddress, SenderGender,SenderContactNo, sessionID, OtherDetails,OperatorID, " +
                                                  "StationID, ReceiverStreet, ReceiverProvince, ReceiverCountry, KPTN, Message, Redeem, BranchCode, SenderBirthdate)" +
                                                  "values('" + ControlNumber + "','" + ReferenceNo + "','" + IRNumber + "','" + Currency + "', '" + Principal  + "','" + Charge + "', " +
                                                  "'" + OtherCharge + "','" + Total + "','" + CancelledDate + "','" + AccountCode + "', now(),'" + CancelledByOperatorID + "', " +
                                                  "'" + CancelledByBranchCode + "','" + CancelledByZoneCode + "','" + CancelledByStationID + "', '" + CancelReason + "', " +
                                                  "'" + CancelDetails + "', '" + SenderFName + "','" + SenderLName + "','" + SenderMName + "','" + SenderName + "','" + ReceiverFName + "', " +
                                                  "'" + ReceiverLName + "','" + ReceiverMName + "','" + ReceiverName + "','" + ReceiverAddress + "','" + ReciverGender + "','" + ReceiverContactNo + "', " +
                                                  "'" + ReceiverBdate + "', '" + CancelCharge + "', '" + ChargeTo + "','" + Forex + "','" + TraceNumber + "','" + SenderAddress + "', '" + SenderGender + "','" + SenderContactNo + "', " +
                                                  "'" + SessionID + "', '" + OtherDetails + "', '" + OperatorID + "', '" + StationID + "','" + ReceiverStreet + "', '" + ReceiverProvince + "','" + ReceiverCountry + "','" + KPTN + "','" + Message + "','" + Redeem + "','" + BranchCode + "','" + SenderBirthdate + "');";
                            //command.Parameters.Clear();
                            //command.Parameters.AddWithValue("ControlNo",ControlNumber);
                            //command.Parameters.AddWithValue("RefNumber", ReferenceNo);
                            //command.Parameters.AddWithValue("IRnum", IRNumber);
                            //command.Parameters.AddWithValue("Currency", Currency);
                            //command.Parameters.AddWithValue("Principal", Principal);
                            //command.Parameters.AddWithValue("Charge", Charge);
                            //command.Parameters.AddWithValue("OtherCharge", OtherCharge);
                            //command.Parameters.AddWithValue("Total", Total);
                            //command.Parameters.AddWithValue("CancelledDate", CancelledDate);
                            //command.Parameters.AddWithValue("AccountCode", AccountCode);
                            //command.Parameters.AddWithValue("CancelledByOperatorID", CancelledByOperatorID);
                            //command.Parameters.AddWithValue("CancelledByBranchCode", CancelledByBranchCode);
                            //command.Parameters.AddWithValue("CancelledByZoneCode", CancelledByZoneCode);
                            //command.Parameters.AddWithValue("CancelledByStationID", CancelledByStationID);
                            //command.Parameters.AddWithValue("CancelReason", CancelReason);
                            //command.Parameters.AddWithValue("CancelDetails", CancelDetails);
                            //command.Parameters.AddWithValue("SenderFName", SenderFName);
                            //command.Parameters.AddWithValue("SenderLName", SenderLName);
                            //command.Parameters.AddWithValue("SenderMName", SenderMName);
                            //command.Parameters.AddWithValue("SenderName", SenderName);
                            //command.Parameters.AddWithValue("ReceiverFName", ReceiverFName);
                            //command.Parameters.AddWithValue("ReceiverLName", ReceiverLName);
                            //command.Parameters.AddWithValue("ReceiverMName", ReceiverMName);
                            //command.Parameters.AddWithValue("ReceiverName", ReceiverName);
                            //command.Parameters.AddWithValue("ReceiverAddress", ReceiverAddress);
                            //command.Parameters.AddWithValue("ReceiverGender", ReciverGender);
                            //command.Parameters.AddWithValue("ReceiverContactNo", ReceiverContactNo);
                            //command.Parameters.AddWithValue("ReceiverBDate", ReceiverBdate);
                            //command.Parameters.AddWithValue("CancelCharge", CancelCharge);
                            //command.Parameters.AddWithValue("ChargeTo", ChargeTo);
                            //command.Parameters.AddWithValue("Forex", Forex);
                            //command.Parameters.AddWithValue("TraceNo", TraceNumber);
                            //command.Parameters.AddWithValue("SenderAddress", SenderAddress);
                            //command.Parameters.AddWithValue("SenderGender", SenderGender);
                            //command.Parameters.AddWithValue("SenderContactNo", SenderContactNo);
                            //command.Parameters.AddWithValue("sessionId", SessionID);
                            //command.Parameters.AddWithValue("OtherDetails", OtherDetails);
                            //command.Parameters.AddWithValue("OperatorID", OperatorID);
                            //command.Parameters.AddWithValue("StationID", StationID);
                            //command.Parameters.AddWithValue("ReceiverStreet", ReceiverStreet);
                            //command.Parameters.AddWithValue("ReceiverProvince", ReceiverProvince);
                            //command.Parameters.AddWithValue("ReceiverCountry", ReceiverCountry);
                            //command.Parameters.AddWithValue("KPTN", KPTN);
                            //command.Parameters.AddWithValue("Message", Message);
                            //command.Parameters.AddWithValue("Redeem", Redeem);
                            //command.Parameters.AddWithValue("BranchCode", BranchCode);
                            //command.Parameters.AddWithValue("SenderBirthdate", SenderBirthdate);
                          
                            Int32 result = command.ExecuteNonQuery();
                            if (result == 1)
                            {
                                SendOutControl = ControlNumber;
                              return "1";
                            }  
                            else
                                return "-1";
                            
                            //if (!result.Equals("1"))
                            //{
                            //    //trans.Rollback();
                            //    //con.Close();
                            //    return "-1";
                            //}
                            //else
                            //{
                            //    String SendOutTrans = InsertSendOutTransRef(ReferenceNo, CancelledDate, AccountCode, BatchNum);
                            //    if (!SendOutTrans.Equals("1"))
                            //    {
                            //        //trans.Rollback();
                            //        //con.Close();
                            //        return "-1";
                            //    }
                            //    else
                            //    {
                            //        //trans.Commit();
                            //        //con.Close();
                            //        return "1";
                            //    }
                            //    //trans.Commit();
                            //    //con.Close();
                            //    //return "1";
                            //}
                        //}
                    //}
                //}
            }
            catch (Exception ex)
            {
                return "Error " + passhere + cont  + ex.Message ;
                //return ex.Message;
            }

        }

        private DateTime getServerDatePartner(Boolean isOpenConnection)
        {
            try
            {
                //if (!isOpenConnection)
                //{
                //    using (MySqlConnection conn = dbconPartner.getConnection())
                //    {
                //        if (conn.State == ConnectionState.Closed)
                //        {
                //            conn.Open();
                //        }

                //        using (MySqlCommand command = conn.CreateCommand())
                //        {
                //            DateTime serverdate;
                //            command.CommandText = "Select NOW() as serverdt;";
                //            using (MySqlDataReader Reader = command.ExecuteReader())
                //            {
                //                Reader.Read();
                //                serverdate = Convert.ToDateTime(Reader["serverdt"]);
                //                //Reader.Close();
                //                //conn.Close();
                //                return serverdate;
                //            }
                //        }
                //    }
                //}
                //else
                //{
                    DateTime serverdate;
                    using (command = con.CreateCommand())
                    {
                        command.CommandText = "Select NOW() as serverdt;";
                        MySqlDataReader Reader = command.ExecuteReader();
                        if(Reader.HasRows)
                        {
                            Reader.Read();
                            serverdate = Convert.ToDateTime(Reader["serverdt"]);
                            Reader.Close();
                            return serverdate;
                        }
                        Reader.Close();
                        return DateTime.Now;
                    }
                    
                //}
            }
            catch (Exception ex)
            {
                kplog.Fatal(ex.ToString());
                throw new Exception(ex.Message);
            }
        }
        public String InsertLogTrans(String ReferenceNo, String KPTNo, String accountCode, String stationNo, Int32 zoneCode, String branchCode, String opetorID)
        {
            try
            {
                //        con.Open();
                //using (command = con.CreateCommand())
                //{


                //trans = con.BeginTransaction(IsolationLevel.ReadCommitted);
                MySqlCommand command = con.CreateCommand();
                String pdate = getServerDatePartner(true).ToString("MM-dd").Replace("-", "");

                command.CommandText = "Insert into kpadminpartnerslog.transactionslogs(refno,kptnno,accountCode,action,txndate," +
                                      "stationno,zonecode,branchcode,operatorid,type)values(@refno,@kptnno,@AccountCode,@Action,now(),@stationNo,@zoneCode,@branchCode," +
                                      "@operatorID,@type)";
                //String reftable = "payout" + pdate;
                command.Parameters.AddWithValue("refno", ReferenceNo);
                command.Parameters.AddWithValue("kptnno", KPTNo);
                command.Parameters.AddWithValue("AccountCode", accountCode);
                command.Parameters.AddWithValue("Action", "SENDOUT");
                // SO.command.Parameters.AddWithValue("Isremote",);
                // SO.command.Parameters.AddWithValue("Txtdate",);
                //SO.command.Parameters.AddWithValue("stationCode",stationID);
                command.Parameters.AddWithValue("stationNo", stationNo);
                command.Parameters.AddWithValue("zoneCode", zoneCode);
                command.Parameters.AddWithValue("branchCode", branchCode);
                command.Parameters.AddWithValue("operatorID", opetorID);
                //SO.command.Parameters.AddWithValue("cancelledReason",);
                command.Parameters.AddWithValue("type", "kppartners");
                Int32 res = command.ExecuteNonQuery();

                if (res == 1)
                {
                    //trans.Rollback();
                    //con.Close();
                    return "1";
                }
                else
                {
                    //trans.Commit();
                    //con.Close();
                    return "-1";
                }

                //}
            }
            catch (Exception ex)
            {
                return "-1";
            }
        
        }
        public String InsertPayOutTransRef(String ReferenceNo, String CancelledDate, String AccountCode, String Currency,int TransType)
        {
            //ConnectDB();
            //using (MySqlConnection con = dbconPartner.getConnection())
            //{
                try
                {
            //        con.Open();
                    //using (command = con.CreateCommand())
                    //{
                        

                        //trans = con.BeginTransaction(IsolationLevel.ReadCommitted);
                    
                      MySqlCommand command = con.CreateCommand();
                      String pdate = getServerDatePartner(true).ToString("MM-dd").Replace("-", "");

                        command.CommandText = "Insert into kppartners.potxnref(ReferenceNo, ClaimedDate, CancelledDate, tablereference," +
                                                          "AccountCode,currency,TransactionType)values(@RefNum,now(),@CancelledDate, @RefTable, @RefAccntCode,@currency,@transtype);";
                        String reftable = "payout" + pdate;
                        command.Parameters.AddWithValue("RefNum", ReferenceNo);
                        command.Parameters.AddWithValue("CancelledDate", CancelledDate);
                        command.Parameters.AddWithValue("RefTable", reftable);
                        command.Parameters.AddWithValue("RefAccntCode", AccountCode);
                        command.Parameters.AddWithValue("currency", Currency);
                        command.Parameters.AddWithValue("transtype", TransType);
                        Int32 res = command.ExecuteNonQuery();
                        
                        if (res==1)
                        {
                            //trans.Rollback();
                            //con.Close();
                            return "1";
                        }
                        else
                        {
                            //trans.Commit();
                            //con.Close();
                            return "-1";
                        }

                    //}
                }
                catch (Exception ex)
                {
                    return "-1";
                }

            //}

        }

        public String InsertSendOutTransRef(String ReferenceNo, String CancelledDate, String AccountCode, String BatchNum, String Currency, int TransType)
        {
            //ConnectDB();
            //using (MySqlConnection con = dbconPartner.getConnection())
            //{
                try
                {
            //        con.Open();
                    //using (command = con.CreateCommand())
                    //{
                    //command = con.CreateCommand();
                    MySqlCommand command = con.CreateCommand();
                        String pdate = getServerDatePartner(true).ToString("MM-dd").Replace("-", "");

                        //trans = con.BeginTransaction(IsolationLevel.ReadCommitted);
                        command.CommandText = "Insert into kppartners.sotxnref(ReferenceNo, ClaimedDate, CancelledDate, tablereference," +
                                                          "AccountCode, BatchNo, TransDate,currency,TransactionType)values(@RefNum,now(),@CancelledDate, @RefTable, @RefAccntCode, @BatchNum, now(),@currency,@transtype);";
                        String reftable = "sendout" + pdate;
                        command.Parameters.AddWithValue("RefNum", ReferenceNo);
                       // command.Parameters.AddWithValue("ClaimedDate", ClaimedDate);
                        command.Parameters.AddWithValue("CancelledDate", CancelledDate);
                        command.Parameters.AddWithValue("RefTable", reftable);
                        command.Parameters.AddWithValue("RefAccntCode", AccountCode);
                        command.Parameters.AddWithValue("BatchNum", BatchNum);
                        command.Parameters.AddWithValue("currency", Currency);
                        command.Parameters.AddWithValue("transtype", TransType);
                        //command.Parameters.AddWithValue("TransDate", TransDate);
                        Int32 res = command.ExecuteNonQuery();
                        
                        if (res==1)
                        {
                            //trans.Rollback();
                            //con.Close();
                            return "1";
                        }
                        else
                        {
                            //trans.Commit();
                            //con.Close();
                            return "-1";
                        }

                    //}
                }
                catch (Exception ex)
                {
                    return "-1";
                    //return ex.Message.ToString();
                }
            }

        //}

    }
//}

