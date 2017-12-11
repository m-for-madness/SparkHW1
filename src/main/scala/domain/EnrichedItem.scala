package com.epam.hubd.spark.scala.core.homework.domain

case class EnrichedItem(motelId: String, motelName: String, bidDate: String, loSa: String, price: Double) extends Comparable[EnrichedItem]{
  override def compareTo(o: EnrichedItem): Int = this.price.compareTo(o.price)
  override def toString: String = s"$motelId,$motelName,$bidDate,$loSa,$price"

}