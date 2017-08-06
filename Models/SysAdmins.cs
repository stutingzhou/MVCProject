using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Models
{
    /// <summary>
    /// 管理员实体类
    /// </summary>
    [Serializable]
    public class SysAdmins
    {
        public int LoginId { get; set; }
        public string LoginName { get; set; }
        public string LoginPwd { get; set; }
    }
}
