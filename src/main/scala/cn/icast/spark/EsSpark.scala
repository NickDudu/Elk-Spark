package cn.itcast.spark
//author:Nick Hu
//Company:FarFetch
//last update: 2019.01.22
//Version 1.0
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
import org.apache.spark._
import sys.process._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.{Calendar, Date}
import java.text.SimpleDateFormat
import scala.util.control.Breaks._


object EsSpark {
  def main(args: Array[String]): Unit = {

    //val index = args(0)
    //Connecting ES server
    val spark: SparkSession = SparkSession.builder().appName("test").master("spark://xxxxx:7077").getOrCreate()
    val df = spark.read.format("org.elasticsearch.spark.sql").option("es.nodes.wan.only", "true").option("es.port", "9200").option("es.nodes", "elkserver").option("es.index.auto.create", "true")

    //Getting the indices list and trim out the datetime
    val fullindexlist = Seq("/bin/sh", "-c", "curl 'http://xxxxx:9200/_cat/indices?pretty&v&s=index'  |  awk '{print $3}'").!!
    val indexlist = fullindexlist.replaceAll("[0-9,.]", "")
    //Exclude unnessary indices
    val list = indexlist.split("\\s+").groupBy(w => w).mapValues(_.length).filter(_._2 > 1).map(_._1)
    val flist = list.filterNot(x => x == "monitoring-es--").filterNot(x => x == "watcher-history--")
    //*****val fflist = flist.toList
    //val i = flist.iterator
    //for (e <- fflist) {
    // val result = e
    //fflist.foreach(x => df.option("es.read.field.as.array.include", "fields.tags , beat , tags").load(x + "2019.01.10"))
    //val result = df.option("es.read.field.as.array.include", "fields.tags , beat , tags").load("logstash-haproxy-2018.12.30/haproxy")
    //result.write.parquet("/home/spark/data/logstash-haproxy-2018.12.30.parquet")
    //try{
    //val i = Iterator("logstash-cn-live-iis_log-servicesproduct-", "logstash-prd-infrastructure-security-ossecconsumer-", "logstash-prd-portal-slices-wishlist-", "logstash-prd-sentinel-dragon-resource-product-", "logstash-prd-portal-slices-ordertracking-")
    //val i = Iterator("logstash-cn-live-app_log-contentservic")
    val i = Iterator("logstash-cn-live-app_log-publicapi-")
    try
    {
      while (i.hasNext) {
        val index = i.next()
        val sdf = new SimpleDateFormat("yyyy.MM.dd")
        val cal = Calendar.getInstance();
        //cal.setTime(dateInstance);
        cal.add(java.util.Calendar.DAY_OF_YEAR, -2);
        //println

        for (it <- 1 to 1) {
          cal.add(java.util.Calendar.DAY_OF_YEAR, 1);
          val cal1 = sdf.format(cal.getTime())
          val indexname = index + cal1.toString
          println("indexname: " + indexname)
          if (fullindexlist.contains(indexname)){
            println(indexname + " exists!")
            val result = df.option("es.read.field.as.array.include", "fields.tags , beat , tags").option("es.read.field.exclude","Msg.Data").load(index + cal1.toString)
            // val result = df.option("es.read.field.as.array.include","fields.tags , beat , tags, Msg, Msg.data , Msg.Data.CacheKey , Msg.Data.Data").option("es.read.field.exclude","Msg.Data").load(index + cal1.toString)

            //result.printSchema()om
            result.write.parquet("/home/spark/data/" + index.dropRight(1) + "/" + index + cal1.toString + ".parquet")}
          else{
            println(indexname + " does not exist, will continue processing next one!")
          }

        }

        print("***************Job Complete!**********************")
      }
    }
    catch
      {
        //case t: Throwable => t.printStackTrace()
        case _: Throwable => println("exception ignored")
      }

  }

}
