﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Configuration;

namespace HotelDAL
{
    /// <summary>
    /// 通用数据访问类
    /// </summary>
    public class SQLHelper
    {
        //连接字符串
        private static string connString = ConfigurationManager.ConnectionStrings["connString"].ToString();

        /// <summary>
        /// 执行增删改操作
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public static int Update(string sql)
        {
            SqlConnection conn = new SqlConnection(connString);
            SqlCommand cmd = new SqlCommand(sql, conn);
            conn.Open();
            int result = cmd.ExecuteNonQuery();
            conn.Close();
            return result;
        }
        /// <summary>
        /// 获取一张数据表
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public static DataTable GetTable(string sql)
        {
            using (SqlConnection conn = new SqlConnection(connString))
            {
                try
                {
                    SqlCommand cmd = new SqlCommand(sql, conn);
                    SqlDataAdapter da = new SqlDataAdapter(cmd);
                    DataSet ds = new DataSet();
                    da.Fill(ds);
                    return ds.Tables[0];
                }
                catch (Exception ex)
                {
                    throw ex;
                   
                }
            }
        }

        /// <summary>
        /// 预处理
        /// </summary>
        /// <param name="conn"></param>
        /// <param name="cmd"></param>
        /// <param name="cmdType"></param>
        /// <param name="sqlParams"></param>
        private static void PreparedCommand(SqlConnection conn, SqlCommand cmd, CommandType cmdType, SqlParameter[] sqlParams)
        {
            cmd.CommandType = cmdType;
            if (sqlParams != null)
                cmd.Parameters.AddRange(sqlParams);
            conn.Open();
        }
        /// <summary>
        /// 获取一张数据表
        /// </summary>
        /// <param name="sql">SQL语句或存储过程名称</param>
        /// <param name="cmdType"></param>
        /// <param name="Params">对应的参数</param>
        /// <returns></returns>
        public static DataTable GetTable(string sql, CommandType cmdType, SqlParameter[] sqlParams)
        {
            using (SqlConnection conn = new SqlConnection(connString))
            {
                try
                {
                    SqlCommand cmd = new SqlCommand(sql, conn);
                    PreparedCommand(conn, cmd, cmdType, sqlParams);
                    SqlDataAdapter da = new SqlDataAdapter(cmd);
                    DataSet ds = new DataSet();
                    da.Fill(ds);
                    return ds.Tables[0];
                }
                catch (Exception ex)
                {

                    return null;
                }

            }
        }
        /// <summary>
        /// 返回单一结果的查询
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public static object GetSingleResult(string sql)
        {
            SqlConnection conn = new SqlConnection(connString);
            SqlCommand cmd = new SqlCommand(sql, conn);
            conn.Open();
            object result = cmd.ExecuteScalar();
            conn.Close();
            return result;
        }
        /// <summary>
        /// 返回一个数据集的查询
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public static SqlDataReader GetReader(string sql)
        {
            SqlConnection conn = new SqlConnection(connString);
            SqlCommand cmd = new SqlCommand(sql, conn);
            conn.Open();
            return cmd.ExecuteReader(CommandBehavior.CloseConnection);
        }

        #region 执行带参数的SQL语句

        public static int Update(string sql, SqlParameter[] parameters)
        {
            SqlConnection conn = new SqlConnection(connString);
            SqlCommand cmd = new SqlCommand(sql, conn);
            try
            {
                conn.Open();
                cmd.Parameters.AddRange(parameters);//添加参数
                int result = cmd.ExecuteNonQuery();
                return result;
            }
            catch (Exception ex)
            {
                //写入日志...

                throw ex;
            }
            finally
            {
                conn.Close();
            }
        }
        public static object GetSingleResult(string sql, SqlParameter[] parameters)
        {
            SqlConnection conn = new SqlConnection(connString);
            SqlCommand cmd = new SqlCommand(sql, conn);

            try
            {
                conn.Open();
                cmd.Parameters.AddRange(parameters);//添加参数
                object result = cmd.ExecuteScalar();
                return result;
            }
            catch (Exception ex)
            {
                //写入日志...

                throw ex;
            }
            finally
            {
                conn.Close();
            }
        }
        public static SqlDataReader GetReader(string sql, SqlParameter[] parameters)
        {
            SqlConnection conn = new SqlConnection(connString);
            SqlCommand cmd = new SqlCommand(sql, conn);

            try
            {
                conn.Open();
                cmd.Parameters.AddRange(parameters);//添加参数
                return cmd.ExecuteReader(CommandBehavior.CloseConnection);
            }
            catch (Exception ex)
            {
                //写入日志...
                conn.Close();
                throw ex;
            }
        }

        #endregion
        #region 调用存储过程

        public static int UpdateByProcedure(string procedureName, SqlParameter[] param)
        {
            SqlConnection conn = new SqlConnection(connString);
            SqlCommand cmd = new SqlCommand();
            cmd.Connection = conn;
            try
            {
                conn.Open();
                cmd.CommandType = CommandType.StoredProcedure;//声明当前要执行的是存储过程
                cmd.CommandText = procedureName;//commandText只需要赋值存储过程名称即可
                cmd.Parameters.AddRange(param);//添加存储过程的参数
                int result = cmd.ExecuteNonQuery();
                return result;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                conn.Close();
            }
        }

        /// <summary>
        /// 执行多结果查询（select）
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public static SqlDataReader GetReaderByProcedure(string procedureName, SqlParameter[] param)
        {
            SqlConnection conn = new SqlConnection(connString);
            SqlCommand cmd = new SqlCommand();
            cmd.Connection = conn;
            try
            {
                conn.Open();
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.CommandText = procedureName;
                cmd.Parameters.AddRange(param);
                SqlDataReader objReader =
                    cmd.ExecuteReader(CommandBehavior.CloseConnection);
                return objReader;
            }
            catch (Exception ex)
            {
                conn.Close();
                throw ex;
            }
        }

        #endregion

        #region 启用事务

        /// <summary>
        /// 启用事务执行多条SQL语句
        /// </summary>      
        /// <param name="sqlList">SQL语句列表</param>      
        /// <returns></returns>
        public static bool ExecSQLByTran(List<string> sqlList)
        {
            SqlConnection conn = new SqlConnection(connString);
            SqlCommand cmd = new SqlCommand();
            cmd.Connection = conn;
            conn.Open();
            cmd.Transaction = conn.BeginTransaction();   //开启事务
            try
            {
                foreach (string itemSql in sqlList)//循环提交SQL语句
                {
                    cmd.CommandText = itemSql;
                    cmd.ExecuteNonQuery();
                }
                cmd.Transaction.Commit();  //提交事务
                return true;
            }
            catch (Exception e)
            {
                cmd.Transaction.Rollback();//回滚事务
                throw e;
            }
            finally
            {
                cmd.Transaction = null;
                conn.Close();
            }
        }

        #endregion
    }
}
