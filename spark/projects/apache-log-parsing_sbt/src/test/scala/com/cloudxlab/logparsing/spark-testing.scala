package com.cloudxlab.logparsing

import org.scalatest.FunSuite
import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}

class SampleTest extends FunSuite with SharedSparkContext {
    test("Computing Top Ips") {
        var line1 = "121.242.40.10 - - [03/Aug/2015:06:30:52 -0400] \"POST /mod_pagespeed_beacon?url=http%3A%2F%2Fwww.knowbigdata.com%2Fpage%2Fabout-us HTTP/1.1\" 204 206 \"http://www.knowbigdata.com/page/about-us\" \"Mozilla/5.0 (Windows NT 6.3; WOW64; rv:39.0) Gecko/20100101 Firefox/39.0\""
        var line2 = "::1 - - [11/May/2015:06:44:40 -0400] \"OPTIONS * HTTP/1.0\" 200 125 \"-\" \"Apache/2.4.7 (Ubuntu) PHP/5.5.9-1ubuntu4.7 OpenSSL/1.0.1f (internal dummy connection)\""

        val utils = new Utils

        val list = List(line1, line2)
        val rdd = sc.parallelize(list);

        assert(rdd.count === list.length)   

        val records = utils.getTopIps(rdd, sc, 10)
        assert(records.length === 1)    
        assert(records(0)._1 == "121.242.40.10")
    }
    test("Should remove IP addresses having 1st Octet Decimal more than 126") {
        var line1 = "121.242.40.10 - - [03/Aug/2015:06:30:52 -0400] \"POST /mod_pagespeed_beacon?url=http%3A%2F%2Fwww.knowbigdata.com%2Fpage%2Fabout-us HTTP/1.1\" 204 206 \"http://www.knowbigdata.com/page/about-us\" \"Mozilla/5.0 (Windows NT 6.3; WOW64; rv:39.0) Gecko/20100101 Firefox/39.0\""
        var line2 = "216.113.160.77 - - [03/Aug/2015:06:30:52 -0400] \"POST /mod_pagespeed_beacon?url=http%3A%2F%2Fwww.knowbigdata.com%2Fpage%2Fabout-us HTTP/1.1\" 204 206 \"http://www.knowbigdata.com/page/about-us\" \"Mozilla/5.0 (Windows NT 6.3; WOW64; rv:39.0) Gecko/20100101 Firefox/39.0\""
        val list = List(line1, line2)
        val rdd = sc.parallelize(list);

        val utils = new Utils
        val records = utils.getTopIps(rdd, sc, 10)
        assert(records.length === 1)
        assert(records(0)._1 == "121.242.40.10")
    }
    test("Computing Top Traffic"){
       
       var line1 = "121.242.40.10 - - [03/Aug/2015:06:30:52 -0400] \"POST /mod_pagespeed_beacon?url=http%3A%2F%2Fwww.knowbigdata.com%2Fpage%2Fabout-us HTTP/1.1\" 204 206 \"http://www.knowbigdata.com/page/about-us\" \"Mozilla/5.0 (Windows NT 6.3; WOW64; rv:39.0) Gecko/20100101 Firefox/39.0\""
       var line2 = "216.113.160.77 - - [03/Aug/2015:06:30:52 -0400] \"POST /mod_pagespeed_beacon?url=http%3A%2F%2Fwww.knowbigdata.com%2Fpage%2Fabout-us HTTP/1.1\" 204 206 \"http://www.knowbigdata.com/page/about-us\" \"Mozilla/5.0 (Windows NT 6.3; WOW64; rv:39.0) Gecko/20100101 Firefox/39.0\""
       var line3 = "121.242.40.10 - - [03/Aug/2015:06:30:52 -0400] \"POST /mod_pagespeed_beacon?url=http%3A%2F%2Fwww.knowbigdata.com%2Fpage%2Fabout-us HTTP/1.1\" 204 206 \"http://www.knowbigdata.com/page/about-us\" \"Mozilla/5.0 (Windows NT 6.3; WOW64; rv:39.0) Gecko/20100101 Firefox/39.0\""
       var line4 = "216.113.160.77 - - [03/Jan/2015:11:30:52 -0400] \"POST /mod_pagespeed_beacon?url=http%3A%2F%2Fwww.knowbigdata.com%2Fpage%2Fabout-us HTTP/1.1\" 204 206 \"http://www.knowbigdata.com/page/about-us\" \"Mozilla/5.0 (Windows NT 6.3; WOW64; rv:39.0) Gecko/20100101 Firefox/39.0\""
       var line5 = "216.113.160.77 - - [03/Jan/2015:11:30:52 -0400] \"POST /mod_pagespeed_beacon?url=http%3A%2F%2Fwww.knowbigdata.com%2Fpage%2Fabout-us HTTP/1.1\" 204 206 \"http://www.knowbigdata.com/page/about-us\" \"Mozilla/5.0 (Windows NT 6.3; WOW64; rv:39.0) Gecko/20100101 Firefox/39.0\""

       val list = List(line1, line2, line3, line4, line5)
        val rdd = sc.parallelize(list);

        val utils = new Utils
        var records = utils.getPeakTraffic(rdd, sc, 2, false)
        println(records(0))
        assert(records.length === 2)
        assert(records(0)._1 == "03/Aug/2015 06:30")
        assert(records(0)._2 == 3)
        
        records = utils.getPeakTraffic(rdd, sc, 2, true)
        assert(records.length === 2)
        assert(records(0)._1 == "03/Jan/2015 11:30")
        assert(records(0)._2 == 2)
    println(records)
    }
    test("Get unique Http Codes")
    {
      var line1 = "140.242.40.10 - - [03/Aug/2015:06:30:52 -0400] \"POST /mod_pagespeed_beacon?url=http%3A%2F%2Fwww.knowbigdata.com%2Fpage%2Fabout-us HTTP/1.1\" 204 206 \"http://www.knowbigdata.com/page/about-us\" \"Mozilla/5.0 (Windows NT 6.3; WOW64; rv:39.0) Gecko/20100101 Firefox/39.0\""
      var line2 = "::1 - - [11/May/2015:06:44:40 -0400] \"OPTIONS * HTTP/1.0\" 200 125 \"-\" \"Apache/2.4.7 (Ubuntu) PHP/5.5.9-1ubuntu4.7 OpenSSL/1.0.1f (internal dummy connection)\""
      var line3 = "204.45.207.113 - - [05/Dec/2014:16:14:53 -0500] \"GET /user/register HTTP/1.1\" 200 6832 \"http://www.knowbigdata.com/node/add\" \"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)"
  
      val utils = new Utils
      val list = List(line1, line2, line3)
      val rdd = sc.parallelize(list);
      val records = utils.getUniqueHttpCodes(rdd, sc)
      assert(records(0)._1 == "200")
      assert(records(0)._2 == 1)
      assert(records(1)._1 == "204")
      assert(records(1)._2 == 1)
      
    }
    test("Computing top10 Urls") {
        var line1 = "140.242.40.10 - - [03/Aug/2015:06:30:52 -0400] \"POST /mod_pagespeed_beacon?url=http%3A%2F%2Fwww.knowbigdata.com%2Fpage%2Fabout-us HTTP/1.1\" 204 206 \"http://www.knowbigdata.com/page/about-us\" \"Mozilla/5.0 (Windows NT 6.3; WOW64; rv:39.0) Gecko/20100101 Firefox/39.0\""
        var line2 = "::1 - - [11/May/2015:06:44:40 -0400] \"OPTIONS * HTTP/1.0\" 200 125 \"-\" \"Apache/2.4.7 (Ubuntu) PHP/5.5.9-1ubuntu4.7 OpenSSL/1.0.1f (internal dummy connection)\""
        var line3 = "204.45.207.113 - - [05/Dec/2014:16:14:53 -0500] \"GET /user/register HTTP/1.1\" 200 6832 \"http://www.knowbigdata.com/node/add\" \"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)"
        var line4 = "140.242.40.10 - - [03/Aug/2015:06:30:52 -0400] \"POST /mod_pagespeed_beacon?url=http%3A%2F%2Fwww.knowbigdata.com%2Fpage%2Fabout-us HTTP/1.1\" 204 206 \"http://www.knowbigdata.com/page/about-us\" \"Mozilla/5.0 (Windows NT 6.3; WOW64; rv:39.0) Gecko/20100101 Firefox/39.0\""
        var line5 = "::1 - - [11/May/2015:06:44:40 -0400] \"OPTIONS * HTTP/1.0\" 200 125 \"-\" \"Apache/2.4.7 (Ubuntu) PHP/5.5.9-1ubuntu4.7 OpenSSL/1.0.1f (internal dummy connection)\""

        val utils = new Utils

        val list = List(line1, line2, line3, line4, line5)
        val rdd = sc.parallelize(list);

        assert(rdd.count === list.length)   

        val records = utils.gettop10urls(rdd, sc, 10)
        assert(records.length === 2)    
        assert(records(0)._1 == "http://www.knowbigdata.com/page/about-us")
        assert(records(0)._2 == 2)
        assert(records(1)._1 == "http://www.knowbigdata.com/node/add")
        assert(records(1)._2 == 1)
    }
}
