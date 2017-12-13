# SparkHW1

Travel recommendation
Could find the best price of given motel at given hour.
Realised on Scala, based on RDD's. Reading and writing to file by rdd
Run locally - change args or chage configuration
To run it on hadoop use this command(add before bids, motels, exchange to ur hdfs):
spark-submit --class main.scala.Main --master local SparkHW.jar bids motels exchange output1

localhost:18080 <- spark jobs history
