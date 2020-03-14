using System;
using System.Data;
using System.Configuration;
using System.Linq;
using System.Web;
using System.Web.Security;
using System.Web.UI;
using System.Web.UI.HtmlControls;
using System.Web.UI.WebControls;
using System.Web.UI.WebControls.WebParts;
using System.Xml.Linq;
using MySql.Data.MySqlClient;
using System.Data.Sql;
using System.Data.SqlClient;
using log4net;

/// <summary>
/// Summary description for DBConnect
/// </summary>
public class DBConnect
{
    private MySqlConnection connection;
    private SqlConnection sqlconnection;
    private Boolean pool = false;
    String path;

    private static readonly ILog kplog = LogManager.GetLogger(typeof(DBConnect));
    //Constructor
    public DBConnect(String Server, String Database, String Username, String Password, String pooling, Int32 maxconn, Int32 minconn, Int32 timeout, String indicator)
    {
        if (indicator.ToUpper().ToString().Equals("MSSQL"))
        {
            InitializeMSSQL(Server, Database, Username, Password, pooling, maxconn, minconn, timeout);
        }
        else
        {
            InitializeMYSQL(Server, Database, Username, Password, pooling, maxconn, minconn, timeout);
        }
    }

    //Initialize values for MYSQL
    private void InitializeMYSQL(String Server, String Database, String Username, String Password, String pooling, Int32 maxconn, Int32 minconn, Int32 timeout)
    {
        try
        {
            
            if (pooling.Equals("1"))
            {
                pool = true;
            }

            string myconstring = "server = " + Server + "; database = " + Database + "; uid = " + Username + ";password= " + Password + "; pooling=" + pool + ";min pool size=" + minconn + ";max pool size=" + maxconn + ";connection lifetime=0; connection timeout=" + timeout + ";Allow Zero Datetime=true";
            connection = new MySqlConnection(myconstring);
        }
        catch (Exception ex)
        {
            kplog.Fatal("Unable to connect", ex);
            throw new Exception(ex.Message);
        }

    }

    //Initialize values for MSSQL
    private void InitializeMSSQL(String Server, String Database, String Username, String Password, String pooling, Int32 maxconn, Int32 minconn, Int32 timeout)
    {
        try
        {

            if (pooling.Equals("1"))
            {
                pool = true;
            }

            string myconstring = "server = " + Server + "; database = " + Database + "; uid = " + Username + ";password= " + Password + "; pooling=" + pool + ";min pool size=" + minconn + ";max pool size=" + maxconn + ";connection lifetime=0; connection timeout=" + timeout + "";
            sqlconnection = new SqlConnection(myconstring);
        }
        catch (Exception ex)
        {
            kplog.Fatal("Unable to connect", ex);
            throw new Exception(ex.Message);
        }
    }

    public String Path
    {
        get { return path; }
        set { path = value; }
    }
    //open connection to database
    public bool OpenConnection()
    {
        try
        {
            connection.Open();
            return true;
        }
        catch (MySqlException)
        {
            //When handling errors, you can your application's response based a
            //on the error number.
            //The two most common error numbers when connecting are as follows:
            //0: Cannot connect to server.
            //1045: Invalid user name and/or password.
            return false;
        }
    }

    //open connection to mssql database
    public bool MSSQLOpenConnection()
    {
        try
        {
            sqlconnection.Open();
            return true;
        }
        catch (SqlException)
        {
            //When handling errors, you can your application's response based a
            //on the error number.
            //The two most common error numbers when connecting are as follows:
            //0: Cannot connect to server.
            //1045: Invalid user name and/or password.
            return false;
        }
    }

    //Close connection
    public bool CloseConnection()
    {
        try
        {
            connection.Close();
            return true;
        }
        catch (MySqlException)
        {
            return false;
        }
    }

    //Close connection to SQL database
    public bool MSSQLCloseConnection()
    {
        try
        {
            sqlconnection.Close();
            return true;
        }
        catch (SqlException)
        {
            return false;
        }
    }

    //Insert statement
    public void Insert()
    {
    }

    //Update statement
    public void Update()
    {
    }

    //Delete statement
    public void Delete()
    {
    }

    public MySqlConnection getConnection()
    {
        return connection;
    }

    public SqlConnection getMSSQLConnection()
    {
        return sqlconnection;
    }

    public void dispose()
    {
        connection.Dispose();
    }

    public void disposeMSSQL()
    {
        sqlconnection.Dispose();
    }

    ////Select statement
    //public List<string>[] Select()
    //{
    //}

    ////Count statement
    //public int Count()
    //{
    //}

    //Backup
    public void Backup()
    {
    }

    //Restore
    public void Restore()
    {
    }
}
