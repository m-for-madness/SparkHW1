package main.scala

import java.text.SimpleDateFormat
import java.util.Date

import com.epam.hubd.spark.scala.core.homework.Constants
import com.epam.hubd.spark.scala.core.homework.domain.{BidError, BidItem, EnrichedItem}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.{PairRDDFunctions, RDD}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Main {
  val ERRONEOUS_DIR: String = "erroneous"
  val AGGREGATED_DIR: String = "aggregated"
  val BIDS_DIVIDED_DIR: String = "bids_divided"
  val BIDS_PATH = "SparkHW/src/main/resources/bids.txt";
  val MOTELS_PATH = "SparkHW/src/main/resources/motels.txt";
  val EXCHANGE_RATE = "SparkHW/src/main/resources/exchange_rate.txt"
  val OUTPUT = "SparkHW/src/main/resources/output"

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Users\\Markiian_Yuskevych\\IdeaProjects\\Projects\\SparkRecommendation")
    val sc = new SparkContext(new SparkConf().setAppName("motels-home-recommendation").setMaster("local"))
    val raw : RDD[String] = sc.textFile(BIDS_PATH)

    processData(sc, BIDS_PATH, MOTELS_PATH, EXCHANGE_RATE, OUTPUT)

    sc.stop()
  }


  def processData(sc: SparkContext, bidsPath: String, motelsPath: String, exchangeRatesPath: String, outputBasePath: String) = {

    /**
      * Task 1:
      * Read the bid data from the provided file.
      */
    val rawBids: RDD[List[String]] = getRawBids(sc, bidsPath)
   // rawBids.saveAsTextFile(VALUES_OUTPUT)
    /**
      * Task 1:
      * Collect the errors and save the result.
      * Hint: Use the BideError case class
      */
     val erroneousRecords: RDD[String] = getErroneousRecords(sc, bidsPath)
     erroneousRecords.saveAsTextFile(s"$outputBasePath/$ERRONEOUS_DIR")

    /**
      * Task 2:
      * Read the exchange rate information.
      * Hint: You will need a mapping between a date/time and rate
      */

    val exchangeRates: Map[String, Double] = getExchangeRates(sc, exchangeRatesPath)
    /**
      * Task 3:
      * Transform the rawBids and use the BidItem case class.
      * - Convert USD to EUR. The result should be rounded to 3 decimal precision.
      * - Convert dates to proper format - use formats in Constants util class
      * - Get rid of records where there is no price for a Losa or the price is not a proper decimal number
      */
    val bids: RDD[BidItem] = getBids(rawBids, exchangeRates)
    bids.saveAsTextFile(s"$outputBasePath/$BIDS_DIVIDED_DIR")
    /**
      * Task 4:
      * Load motels data.
      * Hint: You will need the motels name for enrichment and you will use the id for join
      */
     val motels: Map[String, String] = getMotels(sc, motelsPath)

    /**
      * Task5:
      * Join the bids with motel names and utilize EnrichedItem case class.
      * Hint: When determining the maximum if the same price appears twice then keep the first entity you found
      * with the given price.
      */
     val enriched:RDD[EnrichedItem] = getEnriched(bids, motels)
     enriched.saveAsTextFile(s"$outputBasePath/$AGGREGATED_DIR")
  }

  def getRawBids(sc: SparkContext, bidsPath: String): RDD[List[String]] = {
    val rawBidsRDD:RDD[List[String]] = sc.textFile(bidsPath).filter(s => {
      !s.contains("ERROR")
    }).map(s => {
      s.split(",").toList
    })
    rawBidsRDD
  }

  def getErroneousRecords(sc: SparkContext, bidsPath: String): RDD[String] = {
    val bidsErrorRDD = sc.textFile(bidsPath).filter(x=> x.contains("ERROR")).map(s => {
      s.split(",").toList
    })
   // bidsErrorRDD.foreach(x=> println(x))
    val bidsErrors : RDD[String] = bidsErrorRDD.map(x => (new BidError(x(1),x(2))).toString)
    bidsErrors
  }

  def getExchangeRates(sc: SparkContext, exchangeRatesPath: String): Map[String, Double] = {
    val pairRDD : RDD[(String,Double)] = sc.textFile(exchangeRatesPath).map(x => {
      val arrStrings :Array[String] = x.split(",")
      Tuple2(arrStrings(0).toString,arrStrings(3).toDouble)
    })
    pairRDD.collectAsMap().toMap
  }

  def getBids(rawBids: RDD[List[String]], exchangeRates: Map[String, Double]): RDD[BidItem] = {

    val bidItemList : RDD[BidItem] = rawBids.map(x => {
      var list = mutable.MutableList[BidItem]()
      for(i<-List(5,6,8);if(x(i)!="")){
        if(exchangeRates.contains(x(1))){
          val  simpleDateFormat:SimpleDateFormat = new SimpleDateFormat("HH-dd-MM-yyyy");
          val  date:Date = simpleDateFormat.parse(x(1));
          val ans = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(date)
          list += new BidItem(x(0),ans,Constants.BIDS_HEADER(i),BigDecimal(x(i).toDouble*exchangeRates.get(x(1)).get).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble)
        }
      }
      list.toList
    }).flatMap(x=>x)
    bidItemList
  }

  def getMotels(sc:SparkContext, motelsPath: String): Map[String, String] = {
    val motelsRecords: RDD[(String,String)] = sc.textFile(motelsPath).map(x=>{
      val arrStrings = x.split(",")
      Tuple2(arrStrings(0),arrStrings(1))
     // (arrStrings(0)->arrStrings(1))
    })

    motelsRecords.collectAsMap().toMap
  }

  def getEnriched(bids: RDD[BidItem], motels: Map[String, String]): RDD[EnrichedItem] = {

    val enrichedRecords : RDD[EnrichedItem]= bids.map(x=> {
      var list = mutable.MutableList[EnrichedItem]()
        if(motels.contains(x.motelId)){
          list+=new EnrichedItem(x.motelId, motels.get(x.motelId).get, x.bidDate,x.loSa, x.price)
        }
      list.toList.head
    })
    val enrichedMaxItem = enrichedRecords.groupBy(x=>x.motelId).map(v=>v._2.max)
    enrichedMaxItem
  }
}
