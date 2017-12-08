package com.epam.hubd.spark.scala.core.homework.domain

case class BidError(date: String, errorMessage: String) extends Serializable {

  override def toString: String = s"$date,$errorMessage"
}
