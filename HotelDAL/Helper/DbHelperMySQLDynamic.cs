using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace DAL
{
    /// <summary>
    /// DbHelperMySQL动态类
    /// </summary>
    public class DbHelperMySQLDynamic
    {
        private MySqlConnection mysqlconn;//mysql连接
        private string strCon;//连接串


        #region base
        /// <summary>
        /// 数据库连接串
        /// </summary>
        /// <param name="strcon"></param>
        public DbHelperMySQLDynamic(string strConnect)
        {
            strCon = strConnect;
        }

        /// <summary>
        /// 判断数据库连接是否能够打开
        /// </summary>
        /// <returns></returns>
        public bool IsMySqlConnCanOpen
        {
            get
            {
                if (mysqlconn != null)
                    mysqlconn.Close();
                try
                {
                    mysqlconn = new MySqlConnection(strCon);
                    mysqlconn.Open();
                }
                catch
                {
                    return false;
                }
                finally
                {
                    mysqlconn.Close();
                }
                return true;
            }
        }

        /// <summary>
        /// 得到最大值
        /// </summary>
        /// <param name="FieldName"></param>
        /// <param name="TableName"></param>
        /// <returns></returns>
        public int GetMaxID(string FieldName, string TableName)
        {
            string strsql = "select max(" + FieldName + ")+1 from " + TableName;
            object obj = GetSingle(strsql);
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
        /// 执行SQL语句，返回影响的记录数
        /// </summary>
        /// <param name="SQLString">SQL语句</param>
        /// <returns>影响的记录数</returns>
        public int ExecuteSql(string SQLString)
        {
            using (MySqlConnection connection = new MySqlConnection(strCon))
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
                        throw new Exception(E.Message);
                    }
                }
            }
        }

        public bool Exists(string strSql, params MySqlParameter[] cmdParms)
        {
            object obj = GetSingle(strSql, cmdParms);
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
        /// 执行SQL语句，返回影响的记录数
        /// </summary>
        /// <param name="SQLString">SQL语句</param>
        /// <returns>影响的记录数</returns>
        public int ExecuteSql(string SQLString, params MySqlParameter[] cmdParms)
        {
            using (MySqlConnection connection = new MySqlConnection(strCon))
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
                        throw new Exception(E.Message);
                    }
                }
            }
        }

        /// <summary>
        /// 执行查询语句，返回DataSet
        /// </summary>
        /// <param name="SQLString">查询语句</param>
        /// <returns>DataSet</returns>
        public DataSet Query(string SQLString)
        {
            using (MySqlConnection connection = new MySqlConnection(strCon))
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
                    throw new Exception(ex.Message);
                }
                return ds;
            }
        }

        /// <summary>
        /// 执行带参数查询语句，返回DataSet
        /// </summary>
        /// <param name="SQLString">查询语句</param>
        /// <param name="cmdParms">MySqlParameter参数</param>
        /// <returns>DataSet</returns>
        public DataSet Query(string SQLString, params MySqlParameter[] cmdParms)
        {
            using (MySqlConnection connection = new MySqlConnection(strCon))
            {
                MySqlCommand cmd = new MySqlCommand();
                PrepareCommand(cmd, connection, null, SQLString, cmdParms);
                using (MySqlDataAdapter da = new MySqlDataAdapter(cmd))
                {
                    DataSet ds = new DataSet();
                    try
                    {
                        da.Fill(ds, "ds");
                        cmd.Parameters.Clear();
                    }
                    catch (MySqlException ex)
                    {
                        throw new Exception(ex.Message);
                    }
                    return ds;
                }
            }
        }


        /// <summary>
        /// 执行一条计算查询结果语句，返回查询结果（object）。
        /// </summary>
        /// <param name="SQLString">计算查询结果语句</param>
        /// <returns>查询结果（object）</returns>
        public object GetSingle(string SQLString)
        {
            using (MySqlConnection connection = new MySqlConnection(strCon))
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
                        connection.Close();
                        throw new Exception(e.Message);
                    }
                }
            }
        }

        /// <summary>
        /// 执行一条计算查询结果语句，返回查询结果（object）。
        /// </summary>
        /// <param name="SQLString">计算查询结果语句</param>
        /// <returns>查询结果（object）</returns>
        public object GetSingle(string SQLString, params MySqlParameter[] cmdParms)
        {
            using (MySqlConnection connection = new MySqlConnection(strCon))
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
                        throw new Exception(e.Message);
                    }
                }
            }
        }

        private void PrepareCommand(MySqlCommand cmd, MySqlConnection conn, MySqlTransaction trans, string cmdText, MySqlParameter[] cmdParms)
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
        #endregion base

        #region 存储过程
        public DataSet ExecutProcedure(string proName, MySqlParameter[] parameters)
        {
            using (MySqlConnection connection = new MySqlConnection(strCon))
            {
                using (MySqlCommand cmd = new MySqlCommand())
                {
                    try
                    {
                        connection.Open();
                        cmd.Connection = connection;
                        cmd.CommandText = proName;
                        cmd.CommandType = CommandType.StoredProcedure;
                        if (parameters != null)
                        {
                            foreach (MySqlParameter parameter in parameters)
                            {
                                cmd.Parameters.Add(parameter);
                            }
                        }

                        using (MySqlDataAdapter da = new MySqlDataAdapter(cmd))
                        {
                            DataSet ds = new DataSet();

                            da.Fill(ds, "ds");
                            cmd.Parameters.Clear();
                            return ds;
                        }
                    }
                    catch (MySqlException e)
                    {
                        connection.Close();
                        throw new Exception(e.Message);
                    }
                }
            }
        }

        /// <summary>
        /// 执行没有返回值的存储过程
        /// </summary>
        /// <param name="proName"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public bool ExeProcedure(string proName, MySqlParameter[] parameters)
        {
            int num;
            using (MySqlConnection connection = new MySqlConnection(strCon))
            {
                using (MySqlCommand cmd = new MySqlCommand())
                {
                    try
                    {
                        connection.Open();
                        cmd.Connection = connection;
                        cmd.CommandText = proName;
                        cmd.CommandType = CommandType.StoredProcedure;
                        if (parameters != null)
                        {
                            foreach (MySqlParameter parameter in parameters)
                            {
                                cmd.Parameters.Add(parameter);
                            }
                        }
                        num = cmd.ExecuteNonQuery();
                    }
                    catch (MySqlException e)
                    {
                        connection.Close();
                        throw new Exception(e.Message);
                    }
                }
            }
            return num > 0 ? true : false;
        }
        #endregion 存储过程

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
        public DataSet GetPageList(string field, string tablename, string where, string orderby, int currentPageIndex, int pageSize, out int total)
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
        public DataSet GetPageList(string field, string tablename, string where, string orderby, int currentPageIndex, int pageSize)
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
        public DataSet GetPageList(string field, string tablename, string where, string orderby, string groupby, int currentPageIndex, int pageSize)
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

        #region 在事物中执行sql
        /// <summary>
        /// add by zxq at 2015-5-19
        /// 在事物中执行多条sql语句
        /// </summary>
        /// <param name="listSql"></param>
        /// <returns></returns>
        public bool ExcuteTranSql(List<string> listSql)
        {
            using (MySqlConnection connection = new MySqlConnection(strCon))
            {
                if (listSql != null)
                {
                    connection.Open();
                    MySqlTransaction trans = connection.BeginTransaction();
                    MySqlCommand cmd = new MySqlCommand();
                    cmd.Connection = connection;
                    cmd.Transaction = trans;
                    try
                    {
                        for (int i = 0; i < listSql.Count; i++)
                        {
                            string strsql = listSql[i].ToString();
                            if (strsql.Trim().Length > 1)
                            {
                                cmd.CommandText = listSql[i];
                                cmd.ExecuteNonQuery();
                            }
                        }
                        trans.Commit();
                        return true;
                    }
                    catch
                    {
                        trans.Rollback();
                        return false;
                    }

                }
                else
                {
                    return false;
                }

            }
        }

        /// <summary>
        /// 带有事物的执行sql
        /// </summary>
        /// <param name="dic"></param>
        /// <returns></returns>
        public bool ExcuteTranSql(Dictionary<string, MySqlParameter[]> dic)
        {
            using (MySqlConnection connection = new MySqlConnection(strCon))
            {
                if (dic != null)
                {
                    connection.Open();
                    MySqlTransaction trans = connection.BeginTransaction();
                    using (MySqlCommand cmd = new MySqlCommand())
                    {
                        cmd.Connection = connection;
                        cmd.Transaction = trans;
                        try
                        {
                            foreach (string key in dic.Keys)
                            {
                                string strsql = key;

                                cmd.CommandText = key;
                                if (dic[key] != null)
                                {
                                    foreach (MySqlParameter para in dic[key])
                                    {
                                        cmd.Parameters.Add(para);
                                    }
                                }
                                if (cmd.ExecuteNonQuery() < 0)
                                {
                                    cmd.Transaction.Rollback();
                                    return false;
                                }
                                cmd.Parameters.Clear();

                            }
                            trans.Commit();
                            return true;
                        }
                        catch
                        {
                            trans.Rollback();
                            return false;
                        }
                        finally
                        {
                            connection.Close();
                        }
                    }

                }
                else
                {
                    return false;
                }

            }
        }
        #endregion 在事物中执行sql
    }
}
