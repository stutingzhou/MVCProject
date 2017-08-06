using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Models
{
    /// <summary>
    /// 客户投诉实体类
    /// </summary>
    [Serializable]
    public class Suggestion
    {
        public int SuggestionId { get; set; }
        public string CustomerName { get; set; }
        public string CustomerDesc { get; set; }
        public string SuggestionDesc { get; set; }
        public DateTime SuggestiontTime{ get; set; }
        public string PhoneNumber { get; set; }
        public string Email { get; set; }
        public string StatusId { get; set; }
    }
}
