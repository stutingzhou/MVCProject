using Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using MySql.Data.MySqlClient;
using DAL;

namespace HotelDAL
{
    public class NewsService
    {

//        #region 查询指定的新闻条数
//        /// <summary>
//        /// 查询指定的新闻条数
//        /// </summary>
//        /// <param name="count"></param>
//        /// <returns></returns>
//        public List<News> GetNews(int count)
//       {
//           string sql = @"SELECT  NewsId,
//                                  NewsTitle,
//                                  NewsContents,
//                                  PublishTime,
//                                  NewsCategory.CategoryId,
//                                  CategoryName 
//                                  FROM News INNER JOIN NewsCategory
//                                  ON  NewsCategory.CategoryId=News.CategoryId 
//                                  ORDER BY PublishTime DESC 
//                                  LIMIT @COUNT";
//          MySqlParameter[] param=new MySqlParameter[]
//          {
//            new MySqlParameter("@COUNT",count)
//          };
//          List<News> list = new List<News>();
//          MySqlDataReader objReader = DbHelperMySQL.ExecuteReader(sql, param);
//         while (objReader.Read()) 
//          {
//              list.Add(new News()
//              {
//                  CategoryId =Convert.ToInt32(objReader["CategoryId"]),
//                  CategoryName = objReader["CategoryName"].ToString(),
//                  NewsContents = objReader["NewsContents"].ToString(),
//                  NewsId =Convert.ToInt32( objReader["NewsId"]),
//                  NewsTitle = objReader["NewsTitle"].ToString(),
//                  PublishTime = Convert.ToDateTime(objReader["PublishTime"])
//              });
//          }
//         objReader.Close();
//         return list;
//      }

//        #endregion

//        #region (新增)发布新闻
//        /// <summary>
//       /// (新增)发布新闻
//       /// </summary>
//       /// <param name="objNews"></param>
//       /// <returns></returns>
//        public int PublishNews(News objNews)
//        {
//            string sql = "INSERT INTO News (NewsTitle,NewsContents,CategoryId) VALUES(@NewsTitle,@NewsContents,@CategoryId)";
//            MySqlParameter[] param = new MySqlParameter[]
//            {
//                new MySqlParameter("@NewsTitle",objNews.NewsTitle),
//                new MySqlParameter("@NewsContents",objNews.NewsContents),
//                new MySqlParameter("@CategoryId",objNews.CategoryId)
//            };
//            return DbHelperMySQL.ExecuteSql(sql, param);
//        }

//        #endregion

//        #region 根据id查询新闻详情
//        /// <summary>
//        /// 根据id查询新闻详情
//        /// </summary>
//        /// <param name="newsId"></param>
//        /// <returns></returns>
//        public News GetNewsById(string newsId)
//        {
//            string sql = "SELECT NewsId,NewsTitle,NewsContents,CategoryId,PublishTime FROM News  WHERE NewsId=@NewsId";
//            MySqlParameter[] param = new MySqlParameter[] 
//            {
//             new MySqlParameter("@NewsId",newsId)
            
//            };
//            News objNews = null;
//            MySqlDataReader objReader = DbHelperMySQL.ExecuteReader(sql, param);
//            if (objReader.Read()) 
//            {
//                objNews = new News() 
//                {
//                    CategoryId = Convert.ToInt32(objReader["CategoryId"]),
//                    CategoryName = objReader["CategoryName"].ToString(),
//                    NewsContents = objReader["NewsContents"].ToString(),
//                    NewsId = Convert.ToInt32(objReader["NewsId"]),
//                    NewsTitle = objReader["NewsTitle"].ToString(),
//                    PublishTime = Convert.ToDateTime(objReader["PublishTime"])
//                };
//            }
//            objReader.Close();
//            return objNews;
//        }

//        #endregion

    }
}
