using MySql.Data.MySqlClient;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Linq;
using System.Text;

namespace DAL
{
    public abstract class DbHelperMySQL
    {
        private static MySqlConnection mysqlconn;
        private static MySqlCommandBuilder mysqlcb;
        //数据库连接字符串(web.config来配置)
        public static string MySqlConnectionString = ConfigurationManager.ConnectionStrings["connString"].ConnectionString;//20151104
        //public static string MySqlConnectionString = GetDbString();//20151104
        public DbHelperMySQL()
        {
        }

        #region 打开连接
        private static void OpenMySqlConn()
        {
            if (mysqlconn != null)
                mysqlconn.Close();
            try
            {
                mysqlconn = new MySqlConnection(MySqlConnectionString);
                mysqlconn.Open();
            }
            catch (MySqlException ex)
            {
            }
        }
        #endregion

        #region 关闭连接
        private static void CloseMySqlConn()
        {
            try
            {
                if (mysqlconn != null)
                    mysqlconn.Close();
            }
            catch (MySqlException ex)
            {
            }
        }
        #endregion

        #region 公用方法

        /// <summary>
        /// 
        /// </summary>
        /// <param name="FieldName"></param>
        /// <param name="TableName"></param>
        /// <returns></returns>
        public static int GetMaxID(string FieldName, string TableName)
        {
            string strsql = "select max(" + FieldName + ")+1 from " + TableName;
            object obj = DbHelperMySQL.GetSingle(strsql);
            if (obj == null)
            {
                return 1;
            }
            else
            {
                return int.Parse(obj.ToString());
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="strSql"></param>
        /// <returns></returns>
        public static bool Exists(string strSql)
        {
            object obj = DbHelperMySQL.GetSingle(strSql);
            int cmdresult;
            if ((Object.Equals(obj, null)) || (Object.Equals(obj, System.DBNull.Value)))
            {
                cmdresult = 0;
            }
            else
            {
                cmdresult = int.Parse(obj.ToString());
            }
            if (cmdresult == 0)
            {
                return false;
            }
            else
            {
                return true;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="strSql"></param>
        /// <param name="cmdParms"></param>
        /// <returns></returns>
        public static bool Exists(string strSql, params MySqlParameter[] cmdParms)
        {
            object obj = DbHelperMySQL.GetSingle(strSql, cmdParms);
            int cmdresult;
            if ((Object.Equals(obj, null)) || (Object.Equals(obj, System.DBNull.Value)))
            {
                cmdresult = 0;
            }
            else
            {
                cmdresult = int.Parse(obj.ToString());
            }
            if (cmdresult == 0)
            {
                return false;
            }
            else
            {
                return true;
            }
        }
        #endregion

        #region  执行简单SQL语句

        /// <summary>
        /// 执行SQL语句，返回影响的记录数



        /// </summary>
        /// <param name="SQLString">SQL语句</param>
        /// <returns>影响的记录数</returns>
        public static int ExecuteSql(string SQLString)
        {
            using (MySqlConnection connection = new MySqlConnection(MySqlConnectionString))
            {
                using (MySqlCommand cmd = new MySqlCommand(SQLString, connection))
                {
                    try
                    {
                        connection.Open();
                        int rows = cmd.ExecuteNonQuery();
                        return rows;
                    }
                    catch (MySqlException E)
                    {
                        connection.Close();
                        //throw new Exception(E.Message);
                        cmd.Dispose();
                        //connection.Close();
                        return 0;
                    }
                }
            }
        }

        /// <summary>
        /// 执行SQL语句，设置命令的执行等待时间
        /// </summary>
        /// <param name="SQLString"></param>
        /// <param name="Times"></param>
        /// <returns></returns>
        public static int ExecuteSqlByTime(string SQLString, int Times)
        {
            using (MySqlConnection connection = new MySqlConnection(MySqlConnectionString))
            {
                using (MySqlCommand cmd = new MySqlCommand(SQLString, connection))
                {
                    try
                    {
                        connection.Open();
                        cmd.CommandTimeout = Times;
                        int rows = cmd.ExecuteNonQuery();
                        return rows;
                    }
                    catch (MySqlException E)
                    {
                        //connection.Close();
                        //throw new Exception(E.Message);
                        cmd.Dispose();
                        connection.Close();
                        return 0;
                    }
                }
            }
        }

        /// <summary>
        /// 执行多条SQL语句，实现数据库事务。



        /// </summary>
        /// <param name="SQLStringList">多条SQL语句</param>		
        public static int ExecuteSqlTran(ArrayList SQLStringList)
        {
            int iflag = 0;
            using (MySqlConnection conn = new MySqlConnection(MySqlConnectionString))
            {
                conn.Open();
                MySqlCommand cmd = new MySqlCommand();
                cmd.Connection = conn;
                MySqlTransaction tx = conn.BeginTransaction();
                cmd.Transaction = tx;
                try
                {
                    for (int n = 0; n < SQLStringList.Count; n++)
                    {
                        string strsql = SQLStringList[n].ToString();
                        if (strsql.Trim().Length > 1)
                        {
                            cmd.CommandText = strsql;
                            cmd.ExecuteNonQuery();
                        }
                    }
                    tx.Commit();
                }
                catch (MySqlException E)
                {
                    iflag = 1;
                    tx.Rollback();
                    //throw new Exception(E.Message);
                    cmd.Dispose();
                    //connection.Close();
                    return iflag;
                }
            }

            return iflag;
        }
        /// <summary>
        /// 执行带一个存储过程参数的的SQL语句。



        /// </summary>
        /// <param name="SQLString">SQL语句</param>
        /// <param name="content">参数内容,比如一个字段是格式复杂的文章，有特殊符号，可以通过这个方式添加</param>
        /// <returns>影响的记录数</returns>
        public static int ExecuteSql(string SQLString, string content)
        {
            using (MySqlConnection connection = new MySqlConnection(MySqlConnectionString))
            {
                MySqlCommand cmd = new MySqlCommand(SQLString, connection);
                MySqlParameter myParameter = new MySqlParameter("@content", SqlDbType.NText);
                myParameter.Value = content;
                cmd.Parameters.Add(myParameter);
                try
                {
                    connection.Open();
                    int rows = cmd.ExecuteNonQuery();
                    return rows;
                }
                catch (MySqlException E)
                {
                    //throw new Exception(E.Message);
                    cmd.Dispose();
                    connection.Close();
                    return 0;
                }
                finally
                {
                    cmd.Dispose();
                    connection.Close();
                }
            }
        }
        /// <summary>
        /// 执行存储过程返还执行结果-实现数据库事务



        /// </summary>
        /// <param name="storedProcName"></param>
        /// <param name="parameters"></param>
        public static int RunProcedureTran(string storedProcName, IDataParameter[] parameters)
        {
            using (MySqlConnection connection = new MySqlConnection(MySqlConnectionString))
            {
                int result = 0;
                connection.Open();

                using (MySqlTransaction trans = connection.BeginTransaction(IsolationLevel.Serializable))
                {
                    MySqlCommand command = BuildIntCommand(connection, storedProcName, parameters);
                    command.Transaction = trans;
                    try
                    {
                        command.ExecuteNonQuery();
                        int.TryParse(command.Parameters["ReturnValue"].Value.ToString(), out result);
                        if (result == 0)
                            trans.Commit();
                        else
                            trans.Rollback();
                    }
                    catch (MySqlException ex)
                    {
                        trans.Rollback();
                        //系统内部错误，写系统日志
                        result = 85;
                    }
                }
                return result;
            }
        }
        /// <summary>
        /// 执行带一个存储过程参数的的SQL语句。



        /// </summary>
        /// <param name="SQLString">SQL语句</param>
        /// <param name="content">参数内容,比如一个字段是格式复杂的文章，有特殊符号，可以通过这个方式添加</param>
        /// <returns>影响的记录数</returns>
        public static object ExecuteSqlGet(string SQLString, string content)
        {
            using (MySqlConnection connection = new MySqlConnection(MySqlConnectionString))
            {
                MySqlCommand cmd = new MySqlCommand(SQLString, connection);
                MySqlParameter myParameter = new MySqlParameter("@content", SqlDbType.NText);
                myParameter.Value = content;
                cmd.Parameters.Add(myParameter);
                try
                {
                    connection.Open();
                    object obj = cmd.ExecuteScalar();
                    if ((Object.Equals(obj, null)) || (Object.Equals(obj, System.DBNull.Value)))
                    {
                        return null;
                    }
                    else
                    {
                        return obj;
                    }
                }
                catch (MySqlException E)
                {
                    //throw new Exception(E.Message);
                    cmd.Dispose();
                    connection.Close();
                    return null;
                }
                finally
                {
                    cmd.Dispose();
                    connection.Close();
                }
            }
        }
        /// <summary>
        /// 向数据库里插入图像格式的字段(和上面情况类似的另一种实例)
        /// </summary>
        /// <param name="strSQL">SQL语句</param>
        /// <param name="fs">图像字节,数据库的字段类型为image的情况</param>
        /// <returns>影响的记录数</returns>
        public static int ExecuteSqlInsertImg(string strSQL, byte[] fs)
        {
            using (MySqlConnection connection = new MySqlConnection(MySqlConnectionString))
            {
                MySqlCommand cmd = new MySqlCommand(strSQL, connection);
                MySqlParameter myParameter = new MySqlParameter("@fs", SqlDbType.Image);
                myParameter.Value = fs;
                cmd.Parameters.Add(myParameter);
                try
                {
                    connection.Open();
                    int rows = cmd.ExecuteNonQuery();
                    return rows;
                }
                catch (MySqlException E)
                {
                    //throw new Exception(E.Message);
                    cmd.Dispose();
                    connection.Close();
                    return 0;
                }
                finally
                {
                    cmd.Dispose();
                    connection.Close();
                }
            }
        }

        /// <summary>
        /// 执行一条计算查询结果语句，返回查询结果（object）。



        /// </summary>
        /// <param name="SQLString">计算查询结果语句</param>
        /// <returns>查询结果（object）</returns>
        public static object GetSingle(string SQLString)
        {
            using (MySqlConnection connection = new MySqlConnection(MySqlConnectionString))
            {
                using (MySqlCommand cmd = new MySqlCommand(SQLString, connection))
                {
                    try
                    {
                        connection.Open();
                        object obj = cmd.ExecuteScalar();
                        if ((Object.Equals(obj, null)) || (Object.Equals(obj, System.DBNull.Value)))
                        {
                            return null;
                        }
                        else
                        {
                            return obj;
                        }
                    }
                    catch (MySqlException e)
                    {
                        cmd.Dispose();
                        connection.Close();
                        return null;
                        //throw new Exception(e.Message);
                    }
                }
            }
        }


        /// <summary>
        /// 执行查询语句，返回MySqlDataReader(使用该方法切记要手工关闭MySqlDataReader和连接)
        /// </summary>
        /// <param name="strSQL">查询语句</param>
        /// <returns>MySqlDataReader</returns>
        public static MySqlDataReader ExecuteReader(string strSQL)
        {
            MySqlConnection connection = new MySqlConnection(MySqlConnectionString);
            MySqlCommand cmd = new MySqlCommand(strSQL, connection);
            try
            {
                connection.Open();
                MySqlDataReader myReader = cmd.ExecuteReader();
                return myReader;
            }
            catch (MySqlException e)
            {
                return null;
                //throw new Exception(e.Message);
            }
            //finally //不能在此关闭，否则，返回的对象将无法使用
            //{
            //	cmd.Dispose();
            //	connection.Close();
            //}	


        }
        /// <summary>
        /// 执行查询语句，返回DataSet
        /// </summary>
        /// <param name="SQLString">查询语句</param>
        /// <returns>DataSet</returns>
        public static DataSet Query(string SQLString)
        {
            using (MySqlConnection connection = new MySqlConnection(MySqlConnectionString))
            {
                DataSet ds = new DataSet();
                try
                {
                    connection.Open();
                    MySqlDataAdapter myda = new MySqlDataAdapter(SQLString, connection);
                    myda.Fill(ds, "ds");
                }
                catch (MySqlException ex)
                {
                    connection.Close();
                    connection.Dispose();
                    //throw new Exception(ex.Message);
                    return null;
                }
                return ds;
            }
        }
        /// <summary>
        /// 执行查询语句，返回DataSet,设置命令的执行等待时间



        /// </summary>
        /// <param name="SQLString"></param>
        /// <param name="Times"></param>
        /// <returns></returns>
        public static DataSet Query(string SQLString, int Times)
        {
            using (MySqlConnection connection = new MySqlConnection(MySqlConnectionString))
            {
                DataSet ds = new DataSet();
                try
                {
                    connection.Open();
                    MySqlDataAdapter command = new MySqlDataAdapter(SQLString, connection);
                    command.SelectCommand.CommandTimeout = Times;
                    command.Fill(ds, "ds");
                }
                catch (MySqlException ex)
                {
                    connection.Close();
                    connection.Dispose();
                    //throw new Exception(ex.Message);
                    return null;
                }
                return ds;
            }
        }



        #endregion

        #region 执行带参数的SQL语句

        /// <summary>
        /// 执行SQL语句，返回影响的记录数



        /// </summary>
        /// <param name="SQLString">SQL语句</param>
        /// <returns>影响的记录数</returns>
        public static int ExecuteSql(string SQLString, params MySqlParameter[] cmdParms)
        {
            using (MySqlConnection connection = new MySqlConnection(MySqlConnectionString))
            {
                using (MySqlCommand cmd = new MySqlCommand())
                {
                    try
                    {
                        PrepareCommand(cmd, connection, null, SQLString, cmdParms);
                        int rows = cmd.ExecuteNonQuery();
                        cmd.Parameters.Clear();
                        return rows;
                    }
                    catch (MySqlException E)
                    {
                        cmd.Dispose();
                        connection.Close();
                        return 0;
                        //throw new Exception(E.Message);
                    }
                }
            }
        }

        /// <summary>
        /// 执行SQL语句，实现数据库事物
        /// </summary>
        /// <param name="SQLString">SQL语句</param>
        /// <returns>影响的记录数</returns>
        public static int ExecuteSqlTran(string SQLString, out string strMsg, params MySqlParameter[] cmdParms)
        {
            int flag = 0;
            strMsg = "";
            using (MySqlConnection connection = new MySqlConnection(MySqlConnectionString))
            {
                connection.Open();
                using (MySqlTransaction trans = connection.BeginTransaction())
                {
                    MySqlCommand cmd = new MySqlCommand();
                    try
                    {
                        PrepareCommand(cmd, connection, trans, SQLString, cmdParms);
                        int rows = cmd.ExecuteNonQuery();
                        cmd.Parameters.Clear();
                        trans.Commit();
                        flag = 1;
                    }
                    catch (MySqlException E)
                    {
                        trans.Rollback();
                        flag = -1;
                        strMsg = E.Message;
                        //throw new Exception(E.Message);
                        cmd.Dispose();
                        connection.Close();
                        return 0;
                    }
                }
            }
            return flag;
        }
        /// <summary>
        /// 执行多条SQL语句，实现数据库事务。



        /// </summary>
        /// <param name="SQLStringList">SQL语句的哈希表（key为sql语句，value是该语句的MySqlParameter[]）</param>
        public static int ExecuteSqlTran(Hashtable SQLStringList, out string strMsg)
        {
            int iflag = 0;
            strMsg = "";
            using (MySqlConnection conn = new MySqlConnection(MySqlConnectionString))
            {
                conn.Open();
                using (MySqlTransaction trans = conn.BeginTransaction())
                {
                    MySqlCommand cmd = new MySqlCommand();
                    try
                    {
                        //循环
                        foreach (DictionaryEntry myDE in SQLStringList)
                        {
                            string cmdText = myDE.Key.ToString();
                            MySqlParameter[] cmdParms = (MySqlParameter[])myDE.Value;
                            PrepareCommand(cmd, conn, trans, cmdText, cmdParms);
                            int val = cmd.ExecuteNonQuery();
                            cmd.Parameters.Clear();
                        }
                        trans.Commit();
                    }
                    catch (MySqlException E)
                    {
                        trans.Rollback();
                        iflag = 1;
                        strMsg = E.Message;
                        //throw new Exception(E.Message);
                        cmd.Dispose();
                        return 0;
                    }
                }
            }
            return iflag;
        }


        /// <summary>
        /// 执行一条计算查询结果语句，返回查询结果（object）。



        /// </summary>
        /// <param name="SQLString">计算查询结果语句</param>
        /// <returns>查询结果（object）</returns>
        public static object GetSingle(string SQLString, params MySqlParameter[] cmdParms)
        {
            using (MySqlConnection connection = new MySqlConnection(MySqlConnectionString))
            {
                using (MySqlCommand cmd = new MySqlCommand())
                {
                    try
                    {
                        PrepareCommand(cmd, connection, null, SQLString, cmdParms);
                        object obj = cmd.ExecuteScalar();
                        cmd.Parameters.Clear();
                        if ((Object.Equals(obj, null)) || (Object.Equals(obj, System.DBNull.Value)))
                        {
                            return null;
                        }
                        else
                        {
                            return obj;
                        }
                    }
                    catch (MySqlException e)
                    {
                        //throw new Exception(e.Message);
                        cmd.Dispose();
                        connection.Close();
                        return null;
                    }
                }
            }
        }

        /// <summary>
        /// 执行查询语句，返回MySqlDataReader (使用该方法切记要手工关闭MySqlDataReader和连接)
        /// </summary>
        /// <param name="strSQL">查询语句</param>
        /// <returns>MySqlDataReader</returns>
        public static MySqlDataReader ExecuteReader(string SQLString, params MySqlParameter[] cmdParms)
        {
            MySqlConnection connection = new MySqlConnection(MySqlConnectionString);
            MySqlCommand cmd = new MySqlCommand();
            try
            {
                PrepareCommand(cmd, connection, null, SQLString, cmdParms);
                MySqlDataReader myReader = cmd.ExecuteReader();
                cmd.Parameters.Clear();
                return myReader;
            }
            catch (MySqlException e)
            {
                //throw new Exception(e.Message);
                cmd.Dispose();
                connection.Close();
                return null;
            }
            //finally //不能在此关闭，否则，返回的对象将无法使用
            //{
            //	cmd.Dispose();
            //	connection.Close();
            //}	

        }

        /// <summary>
        /// 执行查询语句，返回DataSet
        /// </summary>
        /// <param name="SQLString">查询语句</param>
        /// <returns>DataSet</returns>
        public static DataSet Query(string SQLString, params MySqlParameter[] cmdParms)
        {
            return QueryList(SQLString, 0, cmdParms);
        }

        public static DataSet QueryList(string SQLString, int pcount, params MySqlParameter[] cmdParms)
        {
            using (MySqlConnection connection = new MySqlConnection(MySqlConnectionString))
            {
                MySqlCommand cmd = new MySqlCommand();
                try
                {
                    PrepareCommand(cmd, connection, null, SQLString, cmdParms);
                    using (MySqlDataAdapter da = new MySqlDataAdapter(cmd))
                    {
                        DataSet ds = new DataSet();

                        da.Fill(ds, "ds");
                        cmd.Parameters.Clear();
                        return ds;
                    }
                }
                catch (MySqlException ex)
                {
                    connection.Close();
                    connection.Dispose();
                    pcount++;
                    if (pcount < 4)
                    {
                        System.Threading.Thread.Sleep(3000);
                        return QueryList(SQLString, pcount, cmdParms);//出现3306
                    }
                    //throw new Exception(ex.Message);
                    else
                        return null;
                    //return null;
                }
                finally
                {
                    connection.Close();
                    connection.Dispose();

                }
            }
        }



        private static void PrepareCommand(MySqlCommand cmd, MySqlConnection conn, MySqlTransaction trans, string cmdText, MySqlParameter[] cmdParms)
        {
            if (conn.State != ConnectionState.Open)
                conn.Open();
            cmd.Connection = conn;
            cmd.CommandText = cmdText;
            if (trans != null)
                cmd.Transaction = trans;
            cmd.CommandType = CommandType.Text;//cmdType;
            if (cmdParms != null)
            {

                foreach (MySqlParameter parameter in cmdParms)
                {
                    if ((parameter.Direction == ParameterDirection.InputOutput || parameter.Direction == ParameterDirection.Input) &&
                        (parameter.Value == null))
                    {
                        parameter.Value = DBNull.Value;
                    }
                    cmd.Parameters.Add(parameter);
                }
            }


        }

        #endregion

        #region 存储过程操作


        /// <summary>
        /// 返回分页数据集



        /// </summary>
        /// <param name="storedProcName"></param>
        /// <param name="parameters"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public static DataSet RunPageProcedure(string storedProcName, IDataParameter[] parameters, out int count)
        {
            using (MySqlConnection connection = new MySqlConnection(MySqlConnectionString))
            {
                DataSet dataSet = new DataSet();
                connection.Open();
                MySqlDataAdapter sqlDA = new MySqlDataAdapter();
                sqlDA.SelectCommand = BuildQueryCommand(connection, storedProcName, parameters);
                sqlDA.Fill(dataSet);
                //dataSet.Tables.RemoveAt(0);
                count = Convert.ToInt32(sqlDA.SelectCommand.Parameters[8].Value.ToString());
                connection.Close();
                return dataSet;
            }
        }
        /// <summary>
        /// 返回分页数据集



        /// </summary>
        /// <param name="storedProcName"></param>
        /// <param name="parameters"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public static DataSet RunPageProcedures(string storedProcName, IDataParameter[] parameters, out int count)
        {
            using (MySqlConnection connection = new MySqlConnection(MySqlConnectionString))
            {
                DataSet dataSet = new DataSet();
                connection.Open();
                MySqlDataAdapter sqlDA = new MySqlDataAdapter();
                sqlDA.SelectCommand = BuildQueryCommand(connection, storedProcName, parameters);
                sqlDA.Fill(dataSet);
                //dataSet.Tables.RemoveAt(0);
                count = Convert.ToInt32(sqlDA.SelectCommand.Parameters[7].Value.ToString());
                connection.Close();
                return dataSet;
            }
        }
        ///// <summary>
        ///// 执行存储过程  (使用该方法切记要手工关闭MySqlDataReader和连接)
        ///// </summary>
        ///// <param name="storedProcName">存储过程名</param>
        ///// <param name="parameters">存储过程参数</param>
        ///// <returns>MySqlDataReader</returns>
        //public static MySqlDataReader RunProcedure(string storedProcName, IDataParameter[] parameters)
        //{
        //    MySqlConnection connection = new MySqlConnection(MySqlConnectionString);
        //    MySqlDataReader returnReader;
        //    connection.Open();
        //    MySqlCommand command = BuildQueryCommand(connection, storedProcName, parameters);
        //    command.CommandType = CommandType.StoredProcedure;
        //    returnReader = command.ExecuteReader();
        //    //Connection.Close(); 不能在此关闭，否则，返回的对象将无法使用            
        //    return returnReader;

        //}

        /// <summary>
        /// 执行无返回值的存储过程
        /// </summary>
        /// <param name="storedProcName"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public static void RunProcedure(string storedProcName, IDataParameter[] parameters)
        {
            using (MySqlConnection connection = new MySqlConnection(MySqlConnectionString))
            {
                connection.Open();
                MySqlCommand cmd = BuildQueryCommand(connection, storedProcName, parameters);
                cmd.ExecuteNonQuery();
                connection.Close();
            }
        }

        /// <summary>
        /// 执行存储过程
        /// </summary>
        /// <param name="storedProcName">存储过程名</param>
        /// <param name="parameters">存储过程参数</param>
        /// <param name="tableName">DataSet结果中的表名</param>
        /// <returns>DataSet</returns>
        public static DataSet RunProcedureDs(string storedProcName, IDataParameter[] parameters)
        {
            using (MySqlConnection connection = new MySqlConnection(MySqlConnectionString))
            {
                DataSet dataSet = new DataSet();
                connection.Open();
                MySqlDataAdapter sqlDA = new MySqlDataAdapter();
                sqlDA.SelectCommand = BuildQueryCommand(connection, storedProcName, parameters);
                sqlDA.Fill(dataSet);
                connection.Close();
                return dataSet;
            }
        }

        public static DataSet RunProcedure(string storedProcName, IDataParameter[] parameters, string tableName, int Times)
        {
            using (MySqlConnection connection = new MySqlConnection(MySqlConnectionString))
            {
                DataSet dataSet = new DataSet();
                connection.Open();
                MySqlDataAdapter sqlDA = new MySqlDataAdapter();
                sqlDA.SelectCommand = BuildQueryCommand(connection, storedProcName, parameters);
                sqlDA.SelectCommand.CommandTimeout = Times;
                sqlDA.Fill(dataSet, tableName);
                connection.Close();
                return dataSet;
            }
        }


        /// <summary>
        /// 构建 MySqlCommand 对象(用来返回一个结果集，而不是一个整数值)
        /// </summary>
        /// <param name="connection">数据库连接</param>
        /// <param name="storedProcName">存储过程名</param>
        /// <param name="parameters">存储过程参数</param>
        /// <returns>MySqlCommand</returns>
        private static MySqlCommand BuildQueryCommand(MySqlConnection connection, string storedProcName, IDataParameter[] parameters)
        {
            MySqlCommand command = new MySqlCommand(storedProcName, connection);
            command.CommandType = CommandType.StoredProcedure;
            foreach (MySqlParameter parameter in parameters)
            {
                if (parameter != null)
                {
                    // 检查未分配值的输出参数,将其分配以DBNull.Value.
                    if ((parameter.Direction == ParameterDirection.InputOutput || parameter.Direction == ParameterDirection.Input) &&
                        (parameter.Value == null))
                    {
                        parameter.Value = DBNull.Value;
                    }
                    command.Parameters.Add(parameter);
                }
            }

            return command;
        }

        /// <summary>
        /// 执行存储过程，返回影响的行数		
        /// </summary>
        /// <param name="storedProcName">存储过程名</param>
        /// <param name="parameters">存储过程参数</param>
        /// <param name="rowsAffected">影响的行数</param>
        /// <returns></returns>
        public static int RunProcedure(string storedProcName, IDataParameter[] parameters, out int rowsAffected)
        {
            using (MySqlConnection connection = new MySqlConnection(MySqlConnectionString))
            {
                int result;
                connection.Open();
                MySqlCommand command = BuildIntCommand(connection, storedProcName, parameters);
                rowsAffected = command.ExecuteNonQuery();
                result = (int)command.Parameters["ReturnValue"].Value;
                //Connection.Close();
                return result;
            }
        }

        /// <summary>
        /// 创建 MySqlCommand 对象实例(用来返回一个整数值)	
        /// </summary>
        /// <param name="storedProcName">存储过程名</param>
        /// <param name="parameters">存储过程参数</param>
        /// <returns>MySqlCommand 对象实例</returns>
        private static MySqlCommand BuildIntCommand(MySqlConnection connection, string storedProcName, IDataParameter[] parameters)
        {
            MySqlCommand command = BuildQueryCommand(connection, storedProcName, parameters);
            command.Parameters.Add(new MySqlParameter("ReturnValue", MySqlDbType.Int32, 4, ParameterDirection.ReturnValue, false, 0, 0, string.Empty, DataRowVersion.Default, null));
            return command;
        }
        #endregion

        #region 分页
        /// <summary>
        ///  分页数据查询(返回记录总数)
        /// </summary>
        /// <param name="field">需要查询的字符串</param>
        /// <param name="tablename">所查询的表名 </param>
        /// <param name="where"> 查询条件 where 之后到order by之间内容 比如 CreateDate<:CreateDate and ID<:ID </param>
        /// <param name="orderby"> 查询条件order by 之后所跟内容  比如 createdate>2008-4-8 desc</param>
        /// <param name="currentPageIndex">当前页面值</param>
        /// <param name="pageSize">页面大小</param>
        /// <returns> DataSet 表一为数据  表二一个字段total 总记录数</returns>
        public static DataSet GetPageList(string field, string tablename, string where, string orderby, int currentPageIndex, int pageSize, out int total)
        {
            StringBuilder strSql = new StringBuilder();
            StringBuilder totalSql = new StringBuilder("SELECT COUNT(1) FROM " + tablename);
            strSql.AppendFormat("select {0} from {1} ", field, tablename);
            if (!string.IsNullOrEmpty(where.Trim()))
            {
                strSql.Append(" WHERE " + where);
                totalSql.Append(" WHERE " + where);
            }
            if (!string.IsNullOrEmpty(orderby.Trim()))
            {
                strSql.Append(" order by " + orderby);
            }
            strSql.Append(" limit " + (currentPageIndex - 1) * pageSize + "," + pageSize);

            total = Int32.Parse(GetSingle(totalSql.ToString()).ToString());
            return Query(strSql.ToString());
        }

        /// <summary>
        ///  分页数据查询(不返回记录总数)
        /// </summary>
        /// <param name="field">需要查询的字符串</param>
        /// <param name="tablename">所查询的表名 </param>
        /// <param name="where"> 查询条件 where 之后到order by之间内容 比如 CreateDate<:CreateDate and ID<:ID </param>
        /// <param name="orderby"> 查询条件order by 之后所跟内容  比如 createdate>2008-4-8 desc</param>
        /// <param name="currentPageIndex">当前页面值</param>
        /// <param name="pageSize">页面大小</param>
        /// <returns> DataSet 表一为数据  表二一个字段total 总记录数</returns>
        public static DataSet GetPageList(string field, string tablename, string where, string orderby, int currentPageIndex, int pageSize)
        {
            StringBuilder strSql = new StringBuilder();
            strSql.AppendFormat("select {0} from {1} ", field, tablename);
            if (!string.IsNullOrEmpty(where.Trim()))
            {
                strSql.Append(" WHERE " + where);
            }
            if (!string.IsNullOrEmpty(orderby.Trim()))
            {
                strSql.Append(" order by " + orderby);
            }
            strSql.Append(" limit " + (currentPageIndex - 1) * pageSize + "," + pageSize);

            return Query(strSql.ToString());
        }

        /// <summary>
        ///  分页数据查询(不返回记录总数)
        /// </summary>
        /// <param name="field">需要查询的字符串</param>
        /// <param name="tablename">所查询的表名 </param>
        /// <param name="where"> 查询条件 where 之后到order by之间内容 比如 CreateDate<:CreateDate and ID<:ID </param>
        /// <param name="orderby"> 查询条件order by 之后所跟内容  比如 createdate>2008-4-8 desc</param>
        /// <param name="currentPageIndex">当前页面值</param>
        /// <param name="pageSize">页面大小</param>
        /// <returns> DataSet 表一为数据  表二一个字段total 总记录数</returns>
        public static DataSet GetPageList(string field, string tablename, string where, string orderby, string groupby, int currentPageIndex, int pageSize)
        {
            StringBuilder strSql = new StringBuilder();
            strSql.AppendFormat("select {0} from {1} ", field, tablename);
            if (!string.IsNullOrEmpty(where.Trim()))
            {
                strSql.Append(" WHERE " + where);
            }
            if (!string.IsNullOrEmpty(groupby.Trim()))
            {
                strSql.Append(" GROUP by " + groupby);
            }
            if (!string.IsNullOrEmpty(orderby.Trim()))
            {
                strSql.Append(" order by " + orderby);
            }

            strSql.Append(" limit " + (currentPageIndex - 1) * pageSize + "," + pageSize);

            return Query(strSql.ToString());
        }
        #endregion

        /// <summary>
        /// 获取数据库连接字符串
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        private static string GetDbString()
        {
            string strConn = "";
            try
            {
                strConn = ConfigurationManager.ConnectionStrings["connString"].ToString();
                if (strConn != "")
                {
                    //strConn = Differ.Common.DESEncrypt.Decrypt(strConn);
                }
            }
            catch { }

            return strConn;
        }

    }
}
