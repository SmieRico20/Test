using System;
using System.Linq;
using System.Web;
using System.Web.Services;
using System.Web.Services.Protocols;
using System.Xml.Linq;
using System.Net;
using log4net;
using log4net.Config;
using System.IO;
using System.Text;
using MySql.Data.MySqlClient;
using System.Data;
using System.Data.SqlClient;

[WebService(Namespace = "http://tempuri.org/", Name= "MLHUILLIER")]
[WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
// To allow this Web Service to be called from script, using ASP.NET AJAX, uncomment the following line. 
// [System.Web.Script.Services.ScriptService]
public class Service : System.Web.Services.WebService
{
    private static readonly ILog customlog = LogManager.GetLogger(typeof(Service));
    //String ls_connectionString;
    SqlConnection connect = null;
    private DBConnect con;
    //String strcon;

    [WebMethod]
    public String[] SysAdTestMe(String ZoneCode)
    {
        //log4net.Config.XmlConfigurator.Configure();
        String[] connected = new String[2];
        connectDB(ZoneCode.ToUpper());
        try
        {
            using (connect = con.getMSSQLConnection())
            {
                connect.Open();
                if (connect.State == ConnectionState.Open)
                {
                    connect.Close();
                    customlog.Info("Successfully connected to " + ZoneCode + " database server");
                    connected[0] = "Successfully connected to " + ZoneCode + " database server";
                }
            }
        }
         catch (Exception ex)
        {
            customlog.Fatal(ex.ToString());
            connected[0] = "Error in connecting to " + ZoneCode + " database server";
            connected[1] = ex.ToString();
        }
        return connected;
    }

    private void connectDB(String site)
    {
        log4net.Config.XmlConfigurator.Configure();
        try
        {
            IniFile ini = new IniFile(AppDomain.CurrentDomain.BaseDirectory + "\\Configuration.ini");
            String dbName = ini.IniReadValue(site, "DBName");
            String dbUser = ini.IniReadValue(site, "USERNAME");
            String dbPass = ini.IniReadValue(site, "PASSWORD");
            String serverName = ini.IniReadValue(site, "SERVER");
            String pool = ini.IniReadValue(site, "Pool");
            Int32 maxcon = Convert.ToInt32(ini.IniReadValue(site, "MaxConn"));
            Int32 mincon = Convert.ToInt32(ini.IniReadValue(site, "MinConn"));
            Int32 timeout = Convert.ToInt32(ini.IniReadValue(site, "timeout"));
            String indicator = ini.IniReadValue(site, "indicator");
            //ls_connectionString = ("user id=" + dbUser + ";password=" + dbPass + ";data source=" + serverName + ";persist security info=False;initial catalog=" + dbName + ";");
            con = new DBConnect(serverName, dbName, dbUser, dbPass, pool, maxcon, mincon, timeout, indicator);
        }
        catch (Exception ex)
        {
            customlog.Fatal(ex.ToString());
            throw new Exception(ex.ToString());
        }
    }

    [WebMethod]
    public Monitoring Monitor(String BranchCode, String TransactionDate, String Zcode)
    {
        connectDB("SYNERGY " + Zcode.ToUpper());


        customlog.Info("BranchCode: " + BranchCode + " TransactionDate: " + TransactionDate + " ZoneCode: " + Zcode);
        String s_date = TransactionDate;
        if (s_date.Length > 10)
        {
            s_date = s_date.Substring(0, 10);
        }
        s_date = Convert.ToDateTime(s_date).ToString("yyyy-MM-dd");
        DataSet dsGbkmut = new DataSet();
        DataSet EntryGuid = new DataSet("EntryGuid");
        EntryGuid.Tables.Add();
        EntryGuid.Tables[0].Columns.Add("Gbkmut");
        EntryGuid.Tables[0].Columns.Add("Pending");
        try
        {
            using (connect = con.getMSSQLConnection())
            {
                using (SqlCommand comm = connect.CreateCommand())
                {
                    String sql = "select Distinct cast(entryguid as varchar(40)) as EntryGuid from gbkmut where datum = '" + s_date + "' and companycode= '" + BranchCode + "'; " +
                        "select Distinct cast(entryguid as varchar(40)) as EntryGuid from TransactionsPending where transactiondate = '" + s_date + "' and companycode= '" + BranchCode + "';";
                    comm.CommandText = sql;
                    SqlDataAdapter dataadapter = new SqlDataAdapter(comm);
                    dataadapter.Fill(dsGbkmut);
                    if ((dsGbkmut.Tables[0].Rows.Count == 0) && (dsGbkmut.Tables[1].Rows.Count == 0))
                    {
                        return new Monitoring { respcode = 1, message = "No data found!", data = null};
                    }
                    if (!(dsGbkmut.Tables[0].Rows.Count == 0))
                    {
                        for (int x = 0; x <= dsGbkmut.Tables[0].Rows.Count - 1; x++)
                        {
                            EntryGuid.Tables[0].Rows.Add(EntryGuid.Tables[0].NewRow());
                            EntryGuid.Tables[0].Rows[x]["Gbkmut"] = dsGbkmut.Tables[0].Rows[x][0];
                        }
                    }
                    if (!(dsGbkmut.Tables[1].Rows.Count == 0))
                    {
                        for (int y = 0; y <= dsGbkmut.Tables[1].Rows.Count - 1; y++)
                        {
                            if (EntryGuid.Tables[0].Rows.Count <= y)
                            {
                                EntryGuid.Tables[0].Rows.Add(EntryGuid.Tables[0].NewRow());
                            }
                            EntryGuid.Tables[0].Rows[y]["Pending"] = dsGbkmut.Tables[1].Rows[y][0];
                        }
                    }
                }
            }
        }
        catch (Exception ex)
        {
            customlog.Fatal(ex.ToString());
            return new Monitoring { respcode = 1, message = ex.ToString(), data = null };
        }
        return new Monitoring { respcode = 0, message = "Success", data = EntryGuid};
    }

    [WebMethod]
    public Monitoring getRemsPTN(String bcode, String strDate, String strClass_01)
    {
        connectDB("PTNTRADE " + strClass_01.ToUpper());

        customlog.Info("BranchCode: " + bcode + " TransactionDate: " + strDate + " ZoneCode: " + strClass_01);
        DataSet getRemsPTN = new DataSet();
        
        try
        {
            String s_date = strDate;
            if (s_date.Length > 10)
            {
                s_date = s_date.Substring(0, 10);
            }

            String bdate = Convert.ToDateTime(s_date).ToString("yyyy-MM-dd");
            String nxdate = Convert.ToDateTime(s_date).AddDays(1).ToString("yyyy-MM-dd");

            using (connect = con.getMSSQLConnection())
            {
                //connect.Open();
                using (SqlCommand comm = connect.CreateCommand())
                {
                    connect.Open();
                    String sql = "SELECT ID from tbl_PT_Tran where Transdate >= '" + bdate + "' AND transdate< '" + nxdate + "' and division='" + bcode.Trim() + "'" +
                                " UNION ALL " +
                                " SELECT ID from tbl_PT_Tran where pulloutdate >= '" + s_date + "' AND pulloutdate< '" + nxdate + "' and division='" + bcode.Trim() + "'";
                    comm.CommandText = sql;
                    SqlDataAdapter dataadapter = new SqlDataAdapter(comm);
                    dataadapter.Fill(getRemsPTN);
                    connect.Close();

                    if (getRemsPTN.Tables[0].Rows.Count == 0)
                    {
                        return new Monitoring { respcode = 1, message = "No records found!", data = null };
                    }
                }
            }
        }
        catch (Exception ex)
        {
            customlog.Fatal(ex.ToString());
            return new Monitoring { respcode = 1, message = ex.ToString(), data = null };
            //throw new Exception(ex.ToString());
        }
        return new Monitoring { respcode = 0, message = "Success", data = getRemsPTN };        
    }

    [WebMethod]
    public Monitoring getMaxTimeStamp(String bcode, String strDate, String zoneCode)
    {
        connectDB("PTNTRADE " + zoneCode.ToUpper());

        customlog.Info("BranchCode: " + bcode + " TransactionDate: " + strDate + " ZoneCode: " + zoneCode);
        DataSet getMaxStamp = new DataSet();
        try
        {
            if (strDate.Length > 10)
            {
                strDate = strDate.Substring(0, 10);
            }
            String bdate = Convert.ToDateTime(strDate).ToString("yyyy-MM-dd");
            String nxdate = Convert.ToDateTime(strDate).AddDays(1).ToString("yyyy-MM-dd");
            using (connect = con.getMSSQLConnection())
            {
                connect.Open();
                using (SqlCommand comm = connect.CreateCommand())
                {
                    String maxsql = "SELECT CONVERT(char(40),sysguid) as sysguid FROM cstTradeIn_Transactions WHERE syscreated >= '" + bdate + "' AND syscreated < '" + nxdate + "' AND division='" + bcode.Trim() + "'" +
                      " UNION ALL " +
                      "SELECT CONVERT(char(40),sysguid) as sysguid FROM cstTradeIn_Transactions WHERE pull_out_date >= '" + bdate + "' AND pull_out_date < '" + nxdate + "' AND division='" + bcode.Trim() + "'";
                    comm.CommandText = maxsql;
                    SqlDataAdapter maxdataapt = new SqlDataAdapter(comm);
                    maxdataapt.Fill(getMaxStamp);
                    connect.Close();

                    if (getMaxStamp.Tables[0].Rows.Count == 0)
                    {
                        return new Monitoring { respcode = 1, message = "No records found!", data = null };
                    }
                }
            }
        }
        catch (Exception ex)
        {
            customlog.Fatal(ex.ToString());
            return new Monitoring { respcode = 1, message = ex.ToString(), data = null };
        }
        return new Monitoring { respcode = 0, message = "Success", data = getMaxStamp };
    }
}
