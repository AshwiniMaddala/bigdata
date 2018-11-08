package com.cloudxlab.logparsing

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import java.time._
import java.time.format.DateTimeFormatter


class Utils extends Serializable {
    val PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)(.*)" (\d{3}) (\S+)""".r
var LogEntry:Map[String, Int] = Map("Host" -> 1, "TimeStamp" -> 4, "Url" -> 6, "HttpCode" ->8)

    def getUrlComponent(line:String, entry:String):(String)={
    val logEntry = PATTERN.findFirstMatchIn(line)
		var returnEntry = "";
    
  	if (!logEntry.isEmpty)
		{
			val matchedEntries = logEntry.get
    	val entryIndex = LogEntry.getOrElse(entry, -1)
      entry match {
        case "Host"=> {
          if(!matchedEntries.group(entryIndex).isEmpty())
          {
            returnEntry = matchedEntries.group(entryIndex).toString();	
          }
          else returnEntry = "Empty";
          }
        case "TimeStamp"=> { 
          if(!matchedEntries.group(entryIndex).isEmpty())
          {
            val rawDate = matchedEntries.group(entryIndex).toString();
            val fullDateFormat = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss -SSSS")
            val stringDate = fullDateFormat.parse(rawDate)
            val newDateFormat = DateTimeFormatter.ofPattern("dd/MMM/yyyy HH:mm")
            returnEntry = newDateFormat.format(stringDate)
          }
          else returnEntry = "";    
        }
        case "Url"=>{
         val urlPattern = "\\S*(www|http:|https:)\\.\\S+\\.\\S+".r 
         val urlMatches = urlPattern.findAllMatchIn(line)
         if(!urlMatches.isEmpty)
         {
          returnEntry = urlMatches.toList.last.toString().replaceAll("^\"|\"$", "")
         }
         }
        case "HttpCode"=>{          
          if(!matchedEntries.group(entryIndex).isEmpty())
          {
            returnEntry = matchedEntries.group(entryIndex).toString();	
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
        var urlLogs = accessLogs.filter(containsIP(_)).map(getUrlComponent(_, "Url"))
        var frequencies = urlLogs.filter(isValidUrl).map((_,1)).reduceByKey(_ + _);
        var sortedFrequencies = frequencies.sortBy(x => x._2, false)
        return sortedFrequencies.take(topn)
    }
    def getPeakTraffic(accessLogs:RDD[String], sc:SparkContext, topn:Int, ascending:Boolean):Array[(String,Int)] = {
      //get top 5 time frames for high traffic
        var timeStampLogs = accessLogs.map(getUrlComponent(_, "TimeStamp"))
        var trafficTimeTuples = timeStampLogs.map((_,1));
        var frequencies = trafficTimeTuples.reduceByKey(_ + _);
        var sortedFrequencies = frequencies.sortBy(x => x._2, ascending)
        return sortedFrequencies.take(topn)
    }
    def getTopIps(accessLogs:RDD[String], sc:SparkContext, topn:Int):Array[(String,Int)] = {
        //Keep only the lines which have IP
        var ipAccessLogs = accessLogs.filter(containsIP).filter(isClassA(_)).map(getUrlComponent(_, "Host"))
        var frequencies = ipAccessLogs.map((_,1)).reduceByKey(_ + _);
        var sortedFrequencies = frequencies.sortBy(x => x._2, false)
        return sortedFrequencies.take(topn)
    }
    def getUniqueHttpCodes(accessLogs:RDD[String], sc:SparkContext):Array[(String, Int)]={
        var uniqueCodes = accessLogs.filter(containsIP).map(getUrlComponent(_, "HttpCode"))
        var uniqueCodeTuples = uniqueCodes.filter(isValidHttpCode).map((_,1));
        var frequencies = uniqueCodeTuples.reduceByKey(_ + _);
        var sortedFrequencies = frequencies.sortBy(x => x._2, false)
        return sortedFrequencies.collect();
    }
    def isClassA(ip:String):Boolean = {
      ip.split('.')(0).toInt <127 
    }
    def isValidUrl(url:String):Boolean = {
      val pattern = "\\S*(www|http:|https:)\\.\\S+\\.\\S+".r
      val patternMatch = pattern.findAllIn(url)
      return patternMatch.hasNext
    }
    def isValidHttpCode(code:String):Boolean = {
      val pattern = "[1-5][0-9][0-9]".r
      val patternMatch = pattern.findAllIn(code)
      return patternMatch.hasNext
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

        var accessLogs = sc.textFile(args(2))
        val top10 = utils.getTopIps(accessLogs, sc, args(1).toInt)
        println("===== TOP 10 IP Addresses =====")
        println("IP Addess : Count");
        for(i <- top10){
            println(i._1 + " : " + i._2);
        }
        val top10urls = utils.gettop10urls(accessLogs, sc, args(1).toInt)
        println("===== TOP 10 Urls =====")
        println("URL : Count");
        for(i <- top10urls){
            println(i._1 + " : " + i._2)
        }
        val top10TrafficTimes = utils.getPeakTraffic(accessLogs, sc, args(1).toInt, false)
        println("=== Top 10 TrafficTimes ===");
        println("Time : Count");
        for(i <- top10TrafficTimes){
          println(i._1 + " : " + i._2);
        }          

        val bottom10TrafficTimes = utils.getPeakTraffic(accessLogs, sc, args(1).toInt, true)
        println("=== Bottom 10 TrafficTimes ===");
        println("Time : Count");
        for(i <- bottom10TrafficTimes){
          println(i._1 + " : " + i._2);
        } 
        
        val uniqueHttpCodes = utils.getUniqueHttpCodes(accessLogs, sc)
        println("=== Unique Http Codes ===");
        println("Code : Count");
        for(i <- uniqueHttpCodes){
            println(i._1 + " : " + i._2);
        } 
    }
}
