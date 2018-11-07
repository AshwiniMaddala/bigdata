package com.cloudxlab.logparsing

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._


class Utils extends Serializable {
    val PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)(.*)" (\d{3}) (\S+)""".r
var LogEntry:Map[String, Int] = Map("Host" -> 1, "TimeStamp" -> 4, "Url" -> 6, "HttpCode" ->8)

    def getUrlComponent(line:String, entry:String):(String)={
    val res = PATTERN.findFirstMatchIn(line)
		var returnEntry = "";
  	if (!res.isEmpty)
		{
			val m = res.get
    	val entryIndex = LogEntry.getOrElse(entry, -1)
      entry match {
        case "Host"=> {
          if(!m.group(entryIndex).isEmpty())
          {
            returnEntry = m.group(entryIndex).toString();	
          }
          else returnEntry = "Empty";
          }
        case "TimeStamp"=> { 
          if(!m.group(entryIndex).isEmpty())
          {
            returnEntry = m.group(entryIndex).toString().substring(1,17);	
          }
          else returnEntry = "";    
        }
        case "Url"=>{
//          println(line);
//          val urlRes = PATTERN.findAllIn(line)
//          for(i <-urlRes)
//          {
//            println(i)
//          }
          if(!m.group(entryIndex).isEmpty())
          {
            returnEntry = m.group(entryIndex).toString();	
          }
          else returnEntry = "";
        }
        case "HttpCode"=>{          
          if(!m.group(entryIndex).isEmpty())
          {
            returnEntry = m.group(entryIndex).toString();	
          }
          else returnEntry = "-1";
        }
			}
		}
  	return returnEntry;
}
    def containsIP(line:String):Boolean = return line matches "^([0-9\\.]+) .*$"
    def gettop10urls(accessLogs:RDD[String], sc:SparkContext, topn:Int):Array[(String,Int)] = {
        //Return the top 10 visited urls 
        var urlLogs = accessLogs.map(getUrlComponent(_, "Url"))
        var url_tuples = urlLogs.map((_,1));
        var frequencies = url_tuples.reduceByKey(_ + _);
        var sortedfrequencies = frequencies.sortBy(x => x._2, false)
        
        return sortedfrequencies.take(topn)
    }
    def getPeakTraffic(accessLogs:RDD[String], sc:SparkContext, topn:Int, ascending:Boolean):Array[(String,Int)] = {
      //get top 5 time frames for high traffic
        var timeStampLogs = accessLogs.map(getUrlComponent(_, "TimeStamp"))
        var trafficTime_tuples = timeStampLogs.map((_,2));
        var frequencies = trafficTime_tuples.reduceByKey(_ + _);
        var sortedfrequencies = frequencies.sortBy(x => x._2, ascending)
        return sortedfrequencies.take(topn)
    }
    def getTopIps(accessLogs:RDD[String], sc:SparkContext, topn:Int):Array[(String,Int)] = {
        //Keep only the lines which have IP
        var ipaccesslogs = accessLogs.filter(containsIP).filter(isClassA).map(getUrlComponent(_, "Host"))
        var ips_tuples = ipaccesslogs.map((_,1));
        var frequencies = ips_tuples.reduceByKey(_ + _);
        var sortedfrequencies = frequencies.sortBy(x => x._2, false)
        return sortedfrequencies.take(topn)
    }
    def getUniqueHttpCodes(accessLogs:RDD[String], sc:SparkContext):Array[(String, Int)]={
        var uniqueCodes = accessLogs.filter(containsIP).map(getUrlComponent(_, "HttpCode"))
        var uniqueCodeTuples = uniqueCodes.map((_,1));
        var frequencies = uniqueCodeTuples.reduceByKey(_ + _);
        var sortedfrequencies = frequencies.sortBy(x => x._2, false)
        return sortedfrequencies.collect();
    }
    def isClassA(ip:String):Boolean = {
      ip.split('.')(0).toInt <127 
    }
}

object EntryPoint {
    val usage = """
        Usage: EntryPoint <how_many> <file_or_directory_in_hdfs>
        Eample: EntryPoint 10 /data/spark/project/access/access.log.45.gz
    """
    
    def main(args: Array[String]) {
        
        if (args.length != 3) {
            println("Expected:3 , Provided: " + args.length)
            println(usage)
            return;
        }

        var utils = new Utils

        // Create a local StreamingContext with batch interval of 10 second
        val conf = new SparkConf().setAppName("WordCount")
        val sc = new SparkContext(conf);
        sc.setLogLevel("WARN")

        // var accessLogs = sc.textFile("/data/spark/project/access/access.log.45.gz")
        var accessLogs = sc.textFile(args(2))
        val top10 = utils.getTopIps(accessLogs, sc, args(1).toInt)
        println("===== TOP 10 IP Addresses =====")
        for(i <- top10){
            println(i)
        }
        val top10urls = utils.gettop10urls(accessLogs, sc, args(1).toInt)
        println("===== TOP 10 Urls =====")
        println("URL : Count");
        for(i <- top10urls){
            println(i._1 + " : " + i._2)
        }
        val top10TrafficTimes = utils.getPeakTraffic(accessLogs, sc, args(1).toInt, false)
        println("=== Top 10 TrafficTimes ===");
        for(i <- top10TrafficTimes){
          println(i);
        }          

        val bottom10TrafficTimes = utils.getPeakTraffic(accessLogs, sc, args(1).toInt, true)
        println("=== Bottom 10 TrafficTimes ===");
        for(i <- bottom10TrafficTimes){
          println(i);
        } 
        
        val uniqueHttpCodes = utils.getUniqueHttpCodes(accessLogs, sc)
        println("=== Unique Http Codes ===");
        for(i <- uniqueHttpCodes){
          println(i);
        } 
        

    }
}
