﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace Hotel.Controllers
{
    public class CompanyInfoController : Controller
    {
        //
        // GET: /CompanyInfo/

        public ActionResult Index()
        {
            return View("AboutUs");
        }

    }
}
