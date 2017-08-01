package com.openjaw.connectors

import com.typesafe.scalalogging.Logger

import scala.xml.XML

class UTSConnector(host: String, username: String, password: String) {

  val logger = Logger(classOf[UTSConnector])

  def getCities() = {
    val cityURL = getURL(host, "cities", username, password)
    logger.info("URL to get cities from UTS supplier: " + cityURL)
    (XML.load(cityURL) \\ "City")
  }

  def getCities(ustCountryCode: String) = {
    val cityURL = getCityURL(host, ustCountryCode, username, password)
    logger.info("URL to get cities from UTS supplier: " + cityURL)
    (XML.load(cityURL) \\ "City")
  }

  def getCountries() = {
    val countryURL = getURL(host, "countries", username, password)
    logger.info("URL to get countries from UTS supplier: " + countryURL)
    val xml = XML.load(countryURL)
    val nodes = (XML.load(countryURL) \\ "Country")
    nodes
  }

  def hash(str: String) = {
    val m = java.security.MessageDigest.getInstance("MD5")
    val b = str.getBytes("UTF-8")
    m.update(b, 0, b.length)
    new java.math.BigInteger(1, m.digest()).toString(16)
  }

  def getCheckSum(password: String, time: String) = {
    val checkSum = (hash(password) + time).toLowerCase()
    hash(checkSum)
  }

  def getURL(host: String, requestType: String, username: String, password: String) = {
    val timestamp: Long = System.currentTimeMillis() / 1000L
    val checkSum = getCheckSum(password, timestamp.toString)
    val strURL = host + requestType + "?language=en&currency=usd&login=" + username + "&time=" + timestamp + "&checksum=" + checkSum
    strURL
  }

  def getCityURL(host: String, ustCountryCode: String, username: String, password: String) = {
    val timestamp: Long = System.currentTimeMillis() / 1000L
    val checkSum = getCheckSum(password, timestamp.toString)
    val strURL = host +  "cities?country_id=" + ustCountryCode + "&language=en&currency=usd&login=" + username + "&time=" + timestamp + "&checksum=" + checkSum
    strURL
  }

}
